class Redis
  module Helper

    def to_redis_proto(*cmd)
      cmd.inject("*#{cmd.length}\r\n") {|acc, arg|
        acc << "$#{arg.length}\r\n#{arg}\r\n"
      }
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

    def copy_string(pipe, key)
      value = redis.get(key)
      pipe << to_redis_proto("SET", key, value)
    end
  
    def copy_hash(pipe, key)
      redis.hgetall(key).each do |field, value|
        pipe << to_redis_proto("HSET", key, field, value) 
      end
    end
  
    def copy_list(pipe, key)
      redis.lrange(key, 0, -1).each do |value|
        pipe << to_redis_proto("LPUSH", key, value)
      end
    end
  
    def copy_set(pipe, key)
      redis.smembers(key).each do |member|
        pipe << to_redis_proto("SADD", key, member)
      end
    end
  
    def copy_zset(pipe, key)
      redis.zrange(key, 0, -1, :with_scores => true).each_slice(2) do |member, score|
        pipe << to_redis_proto("ZADD", key, score, member)
      end 
    end

  end
end