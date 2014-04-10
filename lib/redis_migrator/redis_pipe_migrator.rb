class Redis
  class PipeMigrator
    include Redis::Helper

    def initialize(old_hosts)
      Thread.current[:redis] = Redis::Distributed.new(old_hosts)
    end

    def redis
      Thread.current[:redis]
    end

    def migrate(node, keys, options)
      pipe = IO.popen("redis-cli -h #{node[:host]} -p #{node[:port]} -n #{node[:db]} --pipe", IO::RDWR)

      keys.each {|key|
        copy_key(pipe, key)

        #remove key from old node
        redis.node_for(key).del(key) unless options[:do_not_remove]
      }

      pipe.close
    end

    # Copy a given Redis key to a Redis pipe
    # @param pipe [IO] a pipe opened  redis-cli --pipe
    # @param key [String] a Redis key that needs to be copied
    def copy_key(pipe, key)
      key_type = redis.type(key)
      return false unless ['list', 'hash', 'string', 'set', 'zset'].include?(key_type)

      self.send("copy_#{key_type}", pipe, key)
    end

    def copy_string(pipe, key)
      value = redis.get(key)
      pipe << to_redis_proto('SET', key, value)
    end

    def copy_hash(pipe, key)
      redis.hgetall(key).each do |field, value|
        pipe << to_redis_proto('HSET', key, field, value)
      end
    end

    def copy_list(pipe, key)
      redis.lrange(key, 0, -1).each do |value|
        pipe << to_redis_proto('LPUSH', key, value)
      end
    end

    def copy_set(pipe, key)
      redis.smembers(key).each do |member|
        pipe << to_redis_proto('SADD', key, member)
      end
    end

    def copy_zset(pipe, key)
      redis.zrange(key, 0, -1, with_scores: true).each do |member, score|
        pipe << to_redis_proto('ZADD', key, score, member)
      end
    end
  end
end
