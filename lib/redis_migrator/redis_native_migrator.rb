class Redis
  class NativeMigrator
    def initialize(old_hosts)
      Thread.current[:redis] = Redis::Distributed.new(old_hosts)
    end

    def redis
      Thread.current[:redis]
    end

    def migrate(node_options, keys, _)
      new_node_options = { host: node_options[:host],
                           port: node_options[:port],
                           db:   node_options[:db] }

      grouped_by_old_nodes = keys.group_by do |key|
        redis.node_for(key)
      end

      grouped_by_old_nodes.each do |old_node, node_keys|
        destination_node_options = new_node_options.merge(
          timeout: 30 # lets add a generous timeout here
        )

        node_keys.each_slice(1000) do |slice|
          old_node.pipelined do
            slice.each do |key|
              migrate_key(old_node, key, destination_node_options)
            end
          end
        end
      end
    end

    def migrate_key(node, key, options)
      if Redis::VERSION > '3.0.4'
        node.migrate(key, options)
      else
        node.migrate([options[:host], options[:port], key, options[:db], options[:timeout]])
      end
    end
  end
end
