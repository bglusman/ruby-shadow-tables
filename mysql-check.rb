#!/usr/bin/ruby -w

  # require "rubygems"
  # export RUBYOPT=rubygems in .bashrc, else require "rubygems"
  require "mysql"
  # todo - use these
  require "logger"
  require "optparse"      # more ruby-ish than getoptlong

  # For now the focus is on developing tools for use with the MySql database.
  # The sql generated is MySql-specific.
  # Therefore we use the MySql driver, and skip the DBI abstraction.

  # a shadow table name matches this pattern
  SHADOW_PATTERN = /(.+)_shadow$/i
  SHADOW_SUFFIX = "_shadow"
  
  # these fields must be present to shadow a table
  # we assume that a type check is not needed
  ID_NAME = "id"
  UPDATE_NAME = "updated_at"

  # The database we are working in
  DATABASE_NAME = "enjyn_qa"

# logging interface
# todo - implement verbosity level, log destination
def log_it( msg, verbosity=0 )
  if verbosity >= 0
    puts msg
    # good enough for now
  end
end

# A TableDescription knows about one table in the current database.
# It can tell if a table is a shadow or if it can be shadowed.
# It can supply formatted strings describing the fields in the form needed
# for creation of the table or related triggers.
class TableDescription
  # the name of this table
  attr_reader :table_name
  # the name of the table being shadowed if this is a shadow table
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
  # a hash of all the field names with the type as value
  attr_reader :fields

  # get and analyze the description of this table
  def initialize( table_name )
    @table_name = table_name
    @shadow_match = SHADOW_PATTERN.match( table_name )
    if @shadow_match
      # then this is a shadow table
      # base_name is name of the table the shadow is following
      @base_name = @shadow_match[1]
      # shadow name is the name of the shadow table - there is none for a shadow
      @shadow_name = nil
    else
      @base_name = table_name
      # this is not a shadow table
    end
    # - get the description of this table
    @desc = $dbh.query( "describe #{table_name}" )
    if @desc.nil?
      @column_count = 0
    else
      @column_count =  @desc.num_rows
    end
    # the 6 description keys available are:
    #  Field - the name of the field
    #  Type - the field data type, and (size) when needed
    #  Null - whether NULL is allowed as a value
    #  Default - the default value when a value is not given
    #  Key - information about use of the field in an index
    #  Extra - other properties, e.g. autoincrement
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
        case info["Field"]
          when ID_NAME
            @has_id = true
          when UPDATE_NAME
            @has_update = true
        end
      end
      @create_fields += " #{info["Field"]} #{info["Type"]},"
      @field_list += " #{info["Field"]},"
      @new_list += " new.#{info["Field"]},"
      @fields[info["Field"]] = info["Type"]
    end
    # remove the final commas
    @create_fields.chop!
    @field_list.chop!
    @new_list.chop!
    # we now know enough to create the shadow name (or not)
    if can_shadow?
      @shadow_name = @base_name + SHADOW_SUFFIX
    else
      # there will not be a shadow table
      @shadow_name = nil
    end
    @desc.free
  end
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
  def extra_fields( other_desc )
    @fields.select{ |name, type| !(other_desc.fields.has_key?( name ))}
  end
  # return a hash of existing fields that have type modifications
  def modified_fields( other_desc )
    @fields.select{ |name, type| (other_desc.fields.has_key?( name )) && !(other_desc.fields.fetch( name ) == type )}
  end
  # return the create insert trigger query string for this table
  def insert_trigger_sql
    trigger_body( "insert" )
  end
  # return the create update trigger query string for this table
  def update_trigger_sql
    trigger_body( "update" )
  end
  # common part of trigger body
  def trigger_body( event )
    trigger_statement  = "CREATE TRIGGER #{DATABASE_NAME}.#{base_name}_#{event} "
    trigger_statement += "AFTER #{event.upcase} ON #{DATABASE_NAME}.#{base_name} FOR EACH ROW "
    trigger_statement += "INSERT INTO #{DATABASE_NAME}.#{shadow_name} ("
    trigger_statement += field_list + " ) VALUES ("
    trigger_statement += new_list + " )"
    return trigger_statement

  end
  # return the drop insert trigger query string for this table
  def drop_insert_sql
    drop_trigger_sql( "insert" )
  end
  # return the drop update trigger query string for this table
  def drop_update_sql
    drop_trigger_sql( "update" )
  end
  # common drop trigger portion
  def drop_trigger_sql( event )
    "DROP TRIGGER IF EXISTS #{DATABASE_NAME}.#{base_name}_#{event}"
  end
end
# this is the main portion of the script
  begin
    # connect to the MySQL server
    #           host, user, password, database
    # todo - read database and other configuration from an external file or command line
    $dbh = Mysql.real_connect("localhost", "root", "3njyn42", DATABASE_NAME )
    # get server version string and display it
#    log_it "Server version: " + $dbh.get_server_info
    $tables = []
    # get the names of all tables in this database
    tablist = $dbh.list_tables
    tablist.each do |table|
 #     log_it table
      next_table = TableDescription.new( table )
=begin
      if next_table.shadow?
        log_it "  shadow of " + next_table.base_name
      end
=end

 #     if next_table.can_shadow?
 #       log_it "  can shadow this table"
 #     end
      $tables << next_table
    end
    #
    # - do shadow maintenance - update existing shadow tables that do not match base table
    # - do shadow creation - create shadow tables that do not exist now
    $tables.each do |tab_desc|
      if tab_desc.can_shadow?
        ix_sh = $tables.index{ |item| item.table_name == tab_desc.shadow_name }
        if ix_sh
          # a shadow table exists now, see if it needs to be revised
          log_it " table #{tab_desc.table_name} has shadow at #{ix_sh}, #{$tables[ix_sh].table_name}"
          need_update = ! ( tab_desc.unchanged?( $tables[ix_sh] ) )
          if need_update
            log_it " shadow table #{$tables[ix_sh].table_name} needs an update"
            regenerate_triggers = false
            # add new fields
            new_fields = tab_desc.extra_fields( $tables[ix_sh] )
            if ! new_fields.empty?
              puts "add"
              p new_fields
              regenerate_triggers = true
            end
            # drop removed fields
            drop_fields = $tables[ix_sh].extra_fields( tab_desc )
            if ! drop_fields.empty?
              puts "drop"
              p drop_fields
              regenerate_triggers = true
            end
            # alter modified types
            modify_fields = tab_desc.modified_fields( $tables[ix_sh] )
            if ! modify_fields.empty?
              puts "modify"
              p modify_fields
            end
            # recreate triggers
            if regenerate_triggers
              puts "regenerate triggers"

 #             $dbh.query( tab_desc.drop_insert_sql )
 #             $dbh.query( tab_desc.insert_trigger_sql )

 #             $dbh.query( tab_desc.drop_update_sql )
 #             $dbh.query( tab_desc.update_trigger_sql )
            end
          end
        else
          # there is no shadow, create it now
          log_it " create #{tab_desc.shadow_name}"
          create_statement  = "CREATE TABLE #{DATABASE_NAME}.#{tab_desc.shadow_name} ("
          create_statement += tab_desc.create_fields + " )"
#          puts create_statement
          $dbh.query( create_statement )

          # todo - we might want to create an index someday

          $dbh.query( tab_desc.drop_insert_sql )
          $dbh.query( tab_desc.insert_trigger_sql )
          
          $dbh.query( tab_desc.drop_update_sql )
          $dbh.query( tab_desc.update_trigger_sql )
        end
      else
        if tab_desc.shadow?
          log_it "table #{tab_desc.table_name} is a shadow table"
        else
          log_it "skipping table #{tab_desc.table_name}, can not shadow"
        end
      end
    end
  rescue Mysql::Error => e
    log_it "Error code: #{e.errno}"
    log_it "Error message: #{e.error}"
    log_it "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
  ensure
    # disconnect from server
    $dbh.close if $dbh
  end
