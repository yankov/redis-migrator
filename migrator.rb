require 'redis'
require 'redis/distributed'
require 'ruby-debug'

class Redis
  class Migrator

    attr_accessor :old_cluster, :new_cluster, :old_hosts, :new_hosts

    def initialize(old_hosts, new_hosts)
      @old_hosts = old_hosts
      @new_hosts = new_hosts
      @old_cluster = Redis::Distributed.new(old_hosts)
      @new_cluster = Redis::Distributed.new(new_hosts)
    end

    class << self
      def redis
        Thread.current[:redis]
      end

      def to_redis_proto(*cmd)
        cmd.inject("*#{cmd.length}\r\n") {|acc, arg|
          acc << "$#{arg.length}\r\n#{arg}\r\n"
        }
      end

      def copy_string(pipe, key)
        value = redis.get(key)
        pipe << to_redis_proto(*["SET", key, value])
      end
    
      def copy_hash(pipe, key)
        redis.hgetall(key).each do |field, value|
          pipe << to_redis_proto(*["HSET", key, field, value]) 
        end
      end
    
      def copy_list(pipe, key)
        redis.lrange(key, 0, -1).each do |value|
          pipe << to_redis_proto(*["LPUSH", key, value])
        end
      end
    
      def copy_set(pipe, key)
        redis.smembers(key).each do |member|
          pipe << to_redis_proto(*["SADD", key, member])
        end
      end
    
      def copy_zset(pipe, key)
        redis.zrange(key, 0, -1, :with_scores => true).each_slice(2) do |member, score|
          pipe << to_redis_proto(*["ZADD", key, score, member])
        end 
      end
    end # class << self


    def changed_keys
      keys = @old_cluster.keys("*")

      keys.inject({}) do |acc, key|
        old_node = @old_cluster.node_for(key).client
        new_node = @new_cluster.node_for(key).client

        if (old_node.host != new_node.host) || (old_node.port != new_node.port)
          hash_key = "redis://#{new_node.host}:#{new_node.port}/#{new_node.db}"
          acc[hash_key] = [] if acc[hash_key].nil?
          acc[hash_key] << key
        end

        acc
      end

    end

    def parse_redis_url(redis_url)
      node = URI(redis_url)
      path = node.path
      db = path[1..-1].to_i rescue 0

      {
        :host => node.host,
        :port => node.port,
        :db   => db
      }
    end

    def migrate_keys(node, keys, options={})
      return false if keys.empty? || keys.nil?
      
      Thread.current[:redis] = Redis::Distributed.new(old_hosts)

      f = IO.popen("redis-cli -h #{node[:host]} -p #{node[:port]} -n #{node[:db]} --pipe", IO::RDWR)

      keys.each {|key|
        copy_key(f, key)
        Redis.redis.node_for(key).del(key) unless options[:do_not_remove]
      }

      f.close
    end

    def migrate_cluster(options={})
      keys_to_migrate = changed_keys
      p keys_to_migrate
      puts "Migrating #{keys_to_migrate.values.flatten.count} keys"
      
      keys_to_migrate.keys.each do |node_url|
        node = parse_redis_url(node_url)
        migrate_keys(node, keys_to_migrate[node_url])
      end

    end

    def copy_key(f, key)
      key_type = old_cluster.type(key)
      return false unless ['list', 'hash', 'string', 'set', 'zset'].include?(key_type)

      Migrator.send("copy_#{key_type}", f, key)
    end

  end # class Migrator
end # class Redis
