require 'active_record/connection_adapters/abstract_adapter'

gem 'teradata-cli', '~> 0.0.3'
require 'teradata-cli'

module ActiveRecord
  module ConnectionHandling
    def teradata_connection(config)
      puts "connecting to teradata..."
    end
  end

  module ConnectionAdapters
    class TeradataAdapter < AbstractAdapter
      def adapter_name
        'Teradata'
      end
    end
  end
end
