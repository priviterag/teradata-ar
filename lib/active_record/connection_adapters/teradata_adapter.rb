require 'active_record/connection_adapters/abstract_adapter'

#gem 'teradata-cli', :path => '~/projects/teradata-cli'
require 'teradata-cli'

class ActiveRecord::Base
  def self.table_name_prefix
    $teradata_table_name_prefix
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

      def initialize(logger, logon_string, config)
        @logon_string = logon_string
        @charset = config[:charset]
        @database = config[:database]
        $teradata_table_name_prefix = config[:table_name_prefix]
        @config = config
        connect
        super @connection, logger
      end

      def adapter_name
        'teradata'
      end

      def connect
        @connection = TeradataCli::Connection.open(@logon_string)
      end
      private :connect

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

    end
  end
end
