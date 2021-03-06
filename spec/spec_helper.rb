require 'rspec'
require_relative "../lib/redis_migrator.rb"
require 'mock_redis'

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

def prefill_cluster(cluster)
  ('a'..'z').to_a.each do |key|
    (1..5).to_a.each {|val| cluster.sadd(key, val)}
  end
end
