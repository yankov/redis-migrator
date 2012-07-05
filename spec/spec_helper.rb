$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), "mock_redis/lib"))

require_relative "../migrator.rb"

# include patched version of mock_redis
# that works with Redis::Distributed
require "mock_redis.rb"

class PipeMock
  def initialize(redis)
    @redis = redis
  end

  def close; true; end

  def <<(val)
    val[0] = val[0].downcase.to_sym
    @redis.send(*val)
  end
end


class Redis
  module Helper
    def to_redis_proto(*cmd)
      cmd
    end
  end
end