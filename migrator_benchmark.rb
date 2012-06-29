require './migrator.rb'
require 'digest'
require 'em-synchrony'
require "em-synchrony/fiber_iterator"
require 'em-hiredis'
require 'redis/distributed'
require './lib/support/hiredis.rb'
require './lib/support/redis_distributed.rb'

class MigratorBenchmark

  attr_reader :redis 

  def initialize(redis_hosts)
    @redis_hosts = redis_hosts 
  end

  def populate_keys(i, num)
    key = ::Digest::MD5.hexdigest(i.to_s)

    num.times do |x|
      @redis.sadd(key, ::Digest::MD5.hexdigest("f" + x.to_s))
    end
  rescue => e
    p e.message
  end


  def populate_cluster(keys_num, size)
    EM.synchrony do
      @redis = Redis::Distributed.new(@redis_hosts)
      @redis.flushdb

      EM::Synchrony::FiberIterator.new(keys_num.times.to_a, 1000).each do |i|
        self.populate_keys(i, size)
      end
    end
  end

end


redis_hosts = ["redis://redis-host2.com:6379/1", "redis://redis-host3.com:6379/1"]

mb = MigratorBenchmark.new(redis_hosts)

mb.populate_cluster(1000, 100)




