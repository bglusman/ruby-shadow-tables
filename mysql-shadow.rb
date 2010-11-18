#!/usr/bin/ruby -w

  # require "rubygems"
  # export RUBYOPT=rubygems in .bashrc, else require "rubygems"
  require 'mysql'         # MySQL database interface
  require 'logger'        # logging interface
  require 'optparse'      # more ruby-ish command line parser than getoptlong
  require 'ostruct'       # to create a flexible option structure

  # For now the focus is on developing shadow table tools for use with the MySQL database.
  # The sql generated is MySQL-specific.
  # Therefore we use the MySQL driver, and skip the DBI abstraction.

# these fields must be present to shadow a table
# we assume that a type check is not needed
ID_NAME = "id"
UPDATE_NAME = "updated_at"

# set default option strings to use
SHADOW_SUFFIX = "_shadow"
DEFAULT_LOGFILE = 'mysql-shadow.log'

  # handle command line arguments and defaults
  class CmdOptions

    # return a structure containing the option settings
    def self.parse(args)

      # The options specified on the command line are collected in *options*.
      # We set default values here.
      options = OpenStruct.new

      # - database connection info
      options.user = 'root'
      options.password = '' # mandatory
      options.schema = ''   # mandatory
      options.host = 'localhost'

      # - logging info and verbosity
      options.logfile =  DEFAULT_LOGFILE
      options.loglevel = 'info'

      # - a test mode switch, that is show what we would do
      options.testmode = false

      # - a different shadow table name pattern
      options.shadow_suffix = SHADOW_SUFFIX

      opts = OptionParser.new do |opts|

        opts.banner = "Usage: mysql-shadow.rb -d DATABASE -p PASSWORD [-cfhstuv]"

        opts.separator ""
        opts.separator "Database connection:"

        opts.on("-c", "--connect [HOSTNAME]",
                "The server connection (default=#{options.host})") do |hn|
          options.host = hn || ''
        end

        opts.on("-u", "--user [USER]",
                "The user account (default=#{options.user})") do |usr|
          options.user = usr || ''
        end

        opts.on("-p", "--password PASSWORD",
                "The account password (REQUIRED)") do |pwd|
          options.password = pwd || ''
        end

        opts.on("-d", "--database SCHEMA",
                "The database schema to shadow (REQUIRED)") do |sch|
          options.schema = sch || ''
        end

        opts.separator ""
        opts.separator "Logging options:"

        opts.on("-f", "--file [LOGFILE]",
                "The log file name (default=#{options.logfile})") do |lf|
          options.logfile = lf || DEFAULT_LOGFILE
        end

        # Optional argument with keyword completion.
        opts.on("-v", "--verbosity [LEVEL]", [:fatal, :error, :warn, :info, :debug],
                "The log verbosity level (default=#{options.loglevel} [fatal,error,warn,info,debug])") do |lev|
          options.loglevel = lev || :debug
        end

        opts.separator ""
        opts.separator "Other options:"
        
        # - test mode boolean switch, if true then just show what we would do
        opts.on("-t", "--[no-]test", "Test mode, just show what we would do (default=#{options.testmode}) ") do |tm|
          options.testmode = tm || false
        end

        # - allow a different shadow table name suffix
        opts.on("-s", "--shadow_suffix [SUFFIX]",
                "The table name suffix that denotes a shadow table (default=#{options.shadow_suffix})") do |ss|
          options.shadow_suffix = ss || SHADOW_SUFFIX
        end

        # print the options summary and exit
        opts.on_tail("-h", "--help", "Show this message") do
          puts opts
          exit
        end

      end
      # having defined the parser, use it
      opts.parse!(args)
      
      # verify that we have the required values, if not show usage and exit
      if ( options.schema == '' || options.password == '' )
        puts opts
        exit
      end

      # return the results
      options
    end # end parse()
  end # class CmdOptions

# A TableDescription knows about one table in the current database schema.
# It can tell if a table is a shadow or if it can be shadowed.
# It can supply formatted strings describing the fields in the form needed
# for creation or alteration of the table or related triggers.
# It can be used to find added, dropped, or modified fields.
class TableDescription
  # the name of this table
  attr_reader :table_name

  # the name of the table being shadowed if this is a shadow table else the table name
  attr_reader :base_name

  # the name of the shadow table if this table can be shadowed
  attr_reader :shadow_name

  # the number of fields in this table
  attr_reader :column_count

  # a description of all the fields in the table in create table form
  attr_reader :create_fields

  # trigger insert source - a list of all the field names
  attr_reader :field_list

  # trigger values source - a list of all the field names with NEW prepended
  attr_reader :new_list

  # a hash of all the field name as the key and the field type as the value
  attr_reader :fields

  # get and analyze the description of this table
  def initialize( table_name )
    @table_name = table_name
    # see if this table name matches the shadow table name pattern
    @shadow_match = $shadow_pattern.match( table_name )
    if @shadow_match
      # then this is a shadow table
      # base_name is name of the table the shadow table is following
      @base_name = @shadow_match[1]
      # shadow name is the name of the shadow table - there is none for a shadow
      @shadow_name = nil
    else
      @base_name = table_name
      # this is not a shadow table
    end
    # - get the description of this table from the database
    @desc = $dbh.query( "describe #{table_name}" )
    if @desc.nil?
      @column_count = 0
      # somehow this table has no columns
    else
      @column_count =  @desc.num_rows
    end
    # the 6 description keys available are:
    #  Field   - value is the name of the field
    #  Type    - value is the field data type, and (size) when used
    #  Null    - whether NULL is allowed as a field value
    #           - for shadow tables we do not need a non-null constraint
    #  Default - the default value when a value is not given
    #           - for shadow tables we do not need a default value
    #  Key     - information about use of the field in an index
    #           - for shadow tables we do not need a primary key for now
    #  Extra   - other properties, e.g. autoincrement
    #           - for shadow tables we do not need autoincrement fields
    #
    # - analyze the properties of this table
    #   we only get one scan of desc, so do it all now
    @has_id = false
    @has_update = false
    @create_fields = ''
    @field_list = ''
    @new_list = ''
    @fields = {}
    @desc.each_hash do |info|
      if !shadow?
        # for a non-shadow table we need to know if it can have a shadow
        # to have a shadow it must have these two fields
        case info["Field"]
          when ID_NAME
            @has_id = true
          when UPDATE_NAME
            @has_update = true
        end
        # these strings are only used if this is not a shadow table
        # it seems easier to create them here than to derive them later
        @create_fields += " #{info["Field"]} #{info["Type"]},"
        @field_list += " #{info["Field"]},"
        @new_list += " new.#{info["Field"]},"
      end
      # we need the fields hash for all tables
      @fields[info["Field"]] = info["Type"]
    end
    if ! @create_fields.empty?
      # if this is non-empty then the others are too
      # remove the final commas
      @create_fields.chop!
      @field_list.chop!
      @new_list.chop!
    end
    # we now know enough to create the shadow name (or not)
    if can_shadow?
      @shadow_name = @base_name + $options.shadow_suffix
    else
      # there will not be a shadow table
      @shadow_name = nil
    end
    @desc.free
  end # done initializing the description
  #
  # true if this is a shadow table
  def shadow?
    ! @shadow_match.nil?
  end
  # true if this table should have a shadow
  def can_shadow?
    @has_id && @has_update
  end
  # true if the structure of the given shadow table matches ours
  def unchanged?( shadow_desc )
    # the same fields must be present and their types must match
    @fields.eql?( shadow_desc.fields )
  end
  # return a hash of fields that we have that are not in the other description
  # we use this to detect both added and dropped fields
  def extra_fields( other_desc )
    @fields.select{ |name, type| !(other_desc.fields.has_key?( name ))}
  end
  # return a hash of existing fields that have different types than the other table
  # the types in the other table (the shadow table) need to be updated
  def modified_fields( other_desc )
    @fields.select{ |name, type| (other_desc.fields.has_key?( name )) && !(other_desc.fields.fetch( name ) == type )}
  end
  #
  # -- trigger creation section
  # return the create insert trigger query string for this non-shadow table
  def insert_trigger_sql
    trigger_body( "insert" )
  end
  # return the create update trigger query string for this non-shadow table
  def update_trigger_sql
    trigger_body( "update" )
  end
  # common part of the create trigger body
  def trigger_body( event )
    trigger_statement  = "CREATE TRIGGER #{$options.schema}.#{base_name}_#{event} "
    trigger_statement += "AFTER #{event.upcase} ON #{$options.schema}.#{base_name} FOR EACH ROW "
    trigger_statement += "INSERT INTO #{$options.schema}.#{shadow_name} ("
    trigger_statement += field_list + " ) VALUES ("
    trigger_statement += new_list + " )"
    return trigger_statement
  end
  # end of trigger creation section
  #
  # -- drop trigger query section
  # return the drop insert trigger query string for this non-shadow table
  def drop_insert_sql
    drop_trigger_sql( "insert" )
  end
  # return the drop update trigger query string for this non-shadow table
  def drop_update_sql
    drop_trigger_sql( "update" )
  end
  # common drop trigger portion
  # note this query does nothing and does not throw an error if the trigger does not exist
  def drop_trigger_sql( event )
    "DROP TRIGGER IF EXISTS #{$options.schema}.#{base_name}_#{event}"
  end
  # end of drop trigger section
  #
  #-- alter table section
  # generate an alter table command on the shadow table of this table
  # with the field syntax specified by list_proc
  # based on the fields in the hash - field name as key, field type as value
  def alter_table_sql( field_hash, &list_proc )
    # make a list of all the fields to include, using the appropriate syntax
    field_list = field_hash.collect( &list_proc )
    field_text = field_list.join
    # remove the final comma
    field_text.chop!
    return "ALTER TABLE #{$options.schema}.#{shadow_name}#{field_text}"
  end
  # end of alter table section
  #
  # -- create table section
  # generate a create table command for the shadow table of this table
  def create_table_sql()
    "CREATE TABLE #{$options.schema}.#{shadow_name} (" + create_fields + " )"
  end
  # end of create table section
end
# end of the table description class

# log level translation to logger class constant
def log_level
  case $options.loglevel
    when "fatal", :fatal
      # FATAL an unhandleable error that results in a program crash
      Logger::FATAL
    when "error", :error
      # ERROR a handleable error condition
      Logger::ERROR
    when "warn", :warn
      # WARN a warning
      Logger::WARN
    when "info", :info
      # INFO generic (useful) information about system operation
      Logger::INFO
    when "debug", :debug
      # DEBUG low-level information for developers
      Logger::DEBUG
    else
      Logger::DEBUG
  end
end

# wrapper on sql execution to implement test mode
def exec_sql( statement )
  $logger.debug( statement )
  $dbh.query( statement ) unless $options.testmode
end
#
# common trigger generation mechanism
#  (only used for tables being shadowed)
def generate_triggers( tab_desc )
  exec_sql( tab_desc.drop_insert_sql )
  exec_sql( tab_desc.insert_trigger_sql )

  exec_sql( tab_desc.drop_update_sql )
  exec_sql( tab_desc.update_trigger_sql )
end

#--- this is the main portion of the script ---

# our database connection
$dbh = nil
# our log object
$logger = nil

  begin
    # process the command line options
    # we can accept:
    # - database connection info - schema and password are required
    # - logging info and verbosity
    # - a test mode switch, if true we just show what we would do
    # - a different shadow table name pattern
    $options = CmdOptions.parse(ARGV)

    # start logging
    logfile = File.open( $options.logfile, "a" )
    $logger = Logger.new( logfile )
    $logger.level = log_level()
    $logger.warn "-- starting new run of mysql-shadow in #{$options.schema} --"
    
    # document the option settings
    $logger.debug $options
    # indicate if we are in test mode
    $logger.warn "running in test mode, no changes will be made" if $options.testmode

    # a shadow table name matches this pattern
    $shadow_pattern = /(.+)#{$options.shadow_suffix}$/i

    # connect to the MySQL server
    $dbh = Mysql.real_connect( $options.host, $options.user, $options.password, $options.schema )

    $logger.debug "Server version: " + $dbh.get_server_info

    # get the names of all tables in this database schema
    tablist = $dbh.list_tables

    # build the descriptions of all these tables
    $tables = []
    tablist.each do |table|
      $tables << TableDescription.new( table )
    end
    #
    # helpers that hold the column syntax needed for alter-table-command generation
    # item is a field hash entry, supplied as a two-element array,
    # item[0] is the key (the field name), item[1] is the value (the field type)
    add_syntax = Proc.new {|item| " ADD COLUMN #{item[0]} #{item[1]},"}
    drop_syntax = Proc.new {|item| " DROP COLUMN #{item[0]},"}
    modify_syntax = Proc.new {|item| " MODIFY COLUMN #{item[0]} #{item[1]},"}

    # - do shadow maintenance - update existing shadow tables that do not match base table
    # - do shadow creation - create shadow tables that do not exist now
    $tables.each do |tab_desc|
      $logger.debug "checking table #{tab_desc.table_name}"
      if tab_desc.can_shadow?
        $logger.debug "table #{tab_desc.table_name} can have a shadow"
        # find the index of the corresponding shadow table if it exists
        ix_sh = $tables.index{ |item| item.table_name == tab_desc.shadow_name }
        if ix_sh
          $logger.info "table #{tab_desc.table_name} has a shadow"
          $logger.debug "table #{tab_desc.table_name} shadow index is #{ix_sh}"

          if ! ( tab_desc.unchanged?( $tables[ix_sh] ) )
            $logger.warn "shadow table #{$tables[ix_sh].table_name} needs an update"
            # it seems simpler and safer to treat add, drop, and modify as separate problems
            # since in most cases only one type will be needed for any table

            new_fields = tab_desc.extra_fields( $tables[ix_sh] )
            if ! new_fields.empty?
              $logger.info "adding #{new_fields.length} field(s) to shadow table #{$tables[ix_sh].table_name}"
              exec_sql( tab_desc.alter_table_sql( new_fields, &add_syntax ) )
            end

            drop_fields = $tables[ix_sh].extra_fields( tab_desc )
            if ! drop_fields.empty?
              $logger.info "dropping #{drop_fields.length} field(s) from shadow table #{$tables[ix_sh].table_name}"
              exec_sql( tab_desc.alter_table_sql( drop_fields, &drop_syntax ) )
            end

            modify_fields = tab_desc.modified_fields( $tables[ix_sh] )
            if ! modify_fields.empty?
              $logger.info "modifying #{modify_fields.length} field type(s) in shadow table #{$tables[ix_sh].table_name}"
              exec_sql( tab_desc.alter_table_sql( modify_fields, &modify_syntax ) )
            end

            if (( ! new_fields.empty? ) || ( ! drop_fields.empty? ))
              # then fields were added and/or dropped, so the triggers are obsolete
              $logger.info "regenerate triggers on #{tab_desc.base_name}"
              generate_triggers( tab_desc )
            end
          end # end of need an update of the shadow table
        else
          $logger.warn "create new shadow table #{tab_desc.shadow_name}"
          exec_sql( tab_desc.create_table_sql() )

          # todo - we might want to create an index someday

          $logger.info "generate triggers on #{tab_desc.base_name}"
          generate_triggers( tab_desc )
        end
      else
        # skip existing shadow tables and tables that can not have a shadow
        if tab_desc.shadow?
          $logger.info "table #{tab_desc.table_name} is a shadow table"
        else
          $logger.warn "skipping table #{tab_desc.table_name}, can not shadow it"
        end
      end # end of check if a table can have a shadow
    end # end of shadow table maintenance and creation
  rescue Mysql::Error => e
    $logger.fatal "Error code: #{e.errno}"
    $logger.fatal "Error message: #{e.error}"
    $logger.fatal "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
  ensure
    # disconnect from server
    $dbh.close if $dbh
    # and close the log
    $logger.close if $logger
  end # end of main section
# end of script