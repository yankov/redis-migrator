require 'redis'
require 'redis/distributed'
require_relative 'redis_migrator/redis_helper'
require_relative 'redis_migrator/redis_pipe_migrator'
require_relative 'redis_migrator/redis_native_migrator'

class Redis
  class Migrator
    include Redis::Helper

    attr_accessor :old_cluster, :new_cluster, :old_hosts, :new_hosts

    def initialize(old_hosts, new_hosts)
      @old_hosts = old_hosts
      @new_hosts = new_hosts
      @old_cluster = Redis::Distributed.new(old_hosts)
      @new_cluster = Redis::Distributed.new(new_hosts)
    end

    def redis
      Thread.current[:redis]
    end

    # Finds redis keys for which migration is needed
    # @return a hash of keys grouped by node they need to be written to
    # @example Returned value
    #   { "redis://host1.com" => ['key1', 'key2', 'key3'],
    #     "redis://host2.com => ['key4', 'key5', 'key6']" }
    def scan_keys
      read_to_end_count = 0
      cluster_cursors = @old_cluster.nodes.map { 0 }

      loop do
        threads = []
        acc = {}
        total_size = 0

        @old_cluster.nodes.each_with_index do |cluster, idx|
          threads << Thread.new do
            cursor = cluster_cursors[idx]

            return if cursor == -1

            result = cluster.scan(cursor, count: 10000)
            print "Old cluster #{cluster.client.host}:#{cluster.client.port} "
            if result[0] != "0"
              cluster_cursors[idx] = result[0].to_i
              puts "cursor: #{cluster_cursors[idx]}"
            else
              cluster_cursors[idx] = -1
              puts "readed to end."
              read_to_end_count += 1
            end
            keys = result[1]

            total_size += keys.count

            keys.each do |key|
              old_node = cluster.client
              new_node = @new_cluster.node_for(key).client

              if (old_node.host != new_node.host) || (old_node.port != new_node.port)
                hash_key = "redis://#{new_node.host}:#{new_node.port}/#{new_node.db}"
                acc[hash_key] = [] if acc[hash_key].nil?
                acc[hash_key] << key
              end
            end
          end
        end # @old_cluster.nodes.each

        threads.each { |t| t.join }

        yield acc, total_size

        # server return cursor 0, it's end.
        break if read_to_end_count == @old_cluster.nodes.count
      end # loop
    end

    # Migrates a given array of keys to a given redis node
    # @param node [Hash] options for redis node keys will be migrated to
    # @param keys [Array] array of keys that need to be migrated
    # @param options [Hash] additional options, such as :do_not_remove => true
    def migrate_keys(node, keys, options={})
      return false if keys.empty? || keys.nil?

      migrator(options[:do_not_remove]).new(old_hosts).migrate(node, keys, options)
    end

    # Runs a migration process for a Redis cluster.
    # @param [Hash] additional options such as :do_not_remove => true
    def run(options={})
      scan_keys do |keys_to_migrate, size|
        puts "Migrating #{size} keys"
        threads = []

        keys_to_migrate.keys.each do |node_url|
          node = parse_redis_url(node_url)

          #spawn a separate thread for each Redis pipe
          threads << Thread.new(node, keys_to_migrate[node_url]) {|node, keys|
            migrate_keys(node, keys, options)
          }
        end

        threads.each{|t| t.join}
      end
    end

    private

    def nodes
      old_cluster.nodes + new_cluster.nodes
    end

    def old_nodes
      @old_nodes ||= nodes.select { |node| node.info['redis_version'].to_f < 2.6 }
    end

    def migrator(keep_original)
      @migrator ||= begin
        if old_nodes.any? || keep_original
          Redis::PipeMigrator
        else
          Redis::NativeMigrator
        end
      end
    end
  end # class Migrator
end # class Redis
