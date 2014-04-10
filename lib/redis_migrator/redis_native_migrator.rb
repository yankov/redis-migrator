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
        old_node.pipelined do
          node_keys.each { |key| old_node.migrate(key, new_node_options) }
        end
      end
    end
  end
end
