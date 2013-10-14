require 'active_record/connection_adapters/abstract_adapter'
require 'arel/visitors/bind_visitor'

#gem 'teradata-cli', :path => '~/projects/teradata-cli'
require 'teradata-cli'

class ActiveRecord::Base
  def self.table_name_prefix
    Rails.configuration.database_configuration[Rails.env]['table_name_prefix']
  end
end

module ActiveRecord
  module ConnectionHandling
    def teradata_connection(config)
      config = config.symbolize_keys
      tdpid = config[:tdpid]
      user = config[:username]
      pass = config[:password]
      account = config[:account]
      # charset = config[:charset]
      logon_string = TeradataCli::LogonString.new(tdpid, user, pass, account)
      ConnectionAdapters::TeradataAdapter.new(logger, logon_string, config)
    end
  end

  module ConnectionAdapters
    class TeradataAdapter < AbstractAdapter

      class BindSubstitution < Arel::Visitors::ToSql
        include Arel::Visitors::BindVisitor

        def visit_Arel_Nodes_Limit o
        end

        def visit_Arel_Nodes_Top o
          "TOP #{o.expr}"
        end
      end

      def initialize(logger, logon_string, config)
        @logon_string = logon_string
        @charset = config[:charset]
        @database = config[:database]
        @config = config
        connect
        super @connection, logger
        @visitor = unprepared_visitor
        configure_connection
      end

      def adapter_name
        'teradata'
      end

      def connect
        @connection = TeradataCli::Connection.open(@logon_string)
      end
      private :connect

      def configure_connection
        execute("DATABASE #{@database}") if @database
      end
      private :configure_connection

      def tables(name = nil)
        sql = "SELECT TABLENAME FROM DBC.TABLES "
        clauses = []
        clauses << "DATABASENAME = '#{@database}'" if @database
        clauses << "TABLENAME = '#{name}'" if name
        clauses << "TABLENAME LIKE '#{$teradata_table_name_prefix}%'" if $teradata_table_name_prefix
        unless clauses.empty?
          sql << " WHERE " + clauses.join(' AND ')
        end
        rs = execute(sql)
        rs.entries.collect {|record| record['TableName'].strip}
      end

      # Executes the SQL statement in the context of this connection.
      def execute(sql, name = nil)
        if name == :skip_logging
          @connection.query(sql)
        else
          log(sql, name) { @connection.query(sql) }
        end
      end

      def exec_query(sql, name = 'SQL', binds = [])
        result = execute(sql, name)
        if result && result.count > 0
          ActiveRecord::Result.new(result.entries[0].keys, result.entries.collect{|r|r.collect{|v|v}})
        else
          ActiveRecord::Result.new([],[])
        end
      end

      # Returns an array of record hashes with the column names as keys and
      # column values as values.
      def select(sql, name = nil, binds = [])
        exec_query(sql, name)
      end

      # Can this adapter determine the primary key for tables not attached
      # to an Active Record class, such as join tables? Backend specific, as
      # the abstract adapter always returns +false+.
      def supports_primary_key?
        true
      end

      def primary_key(table_name)
        return :id
        column = table_structure(table_name).find { |field|
          field['pk'] == 1
        }
        column && column['name']
      end

      # Returns an array of +Column+ objects for the table specified by +table_name+.
      def columns(table_name)#:nodoc:
        sql = "SELECT * FROM DBC.COLUMNS WHERE TABLENAME='#{table_name}'"
        sql << " AND DATABASENAME='#{@database}'" if @database
        rs = execute(sql)
        rs.entries.collect do |record|
          new_column(
            extract_field_name(record['ColumnName']),
            extract_default(record['DefaultValue']),
            extract_field_type(record['ColumnType']),
            extract_nullable(record['Nullable'])
          )
        end
      end

      # Overridden by the adapters to instantiate their specific Column type.
      def new_column(field, default, type, null) # :nodoc:
        Column.new(field, default, type, null)
      end

      def extract_field_name(name)
        name.strip
      end

      def extract_default(default)
        ''
      end

      def extract_field_type(type)
        case type.strip
        when "I", "I1"
          :integer
        when "CV"
          :string
        when "DA"
          :date
        when "D"
          :decimal
        when "TS"
          :datetime
        else
          raise "Column type #{type} not supported"
        end
      end

      def extract_nullable(nullable)
        true
      end

    end
  end
end
