require 'active_record/connection_adapters/abstract_adapter'

#gem 'teradata-cli', :path => '~/projects/teradata-cli'
require 'teradata-cli'

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
        # FIXME: fetch metadata from database
        ['shohins']
      end
    end
  end
end
