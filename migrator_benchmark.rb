require './migrator.rb'
require 'digest'
require 'em-synchrony'
require "em-synchrony/fiber_iterator"
require 'em-hiredis'
require 'redis/distributed'
require './lib/support/hiredis.rb'
require './lib/support/redis_distributed.rb'

class MigratorBenchmark

  attr_accessor :redis, :counter, :loop_size 

  def initialize(redis_hosts)
    @redis_hosts = redis_hosts 
  end

  def redis
    @redis ||= Redis::Distributed.new(@redis_hosts) if EM.reactor_running?
  end

  def measure_time(start_time, message='')
    EM::add_periodic_timer( 2 ) do 
      next if counter < loop_size 
      
      time = Time.now - start_time
      puts "#{message} #{time} seconds"
      EM.stop
    end
  end

  def populate_keys(i, num)
    key = ::Digest::MD5.hexdigest(i.to_s)

    num.times do |x|
      redis.sadd(key, ::Digest::MD5.hexdigest("f" + x.to_s)).callback { self.counter += 1 }
    end
  rescue => e
    p e.message
  end


  def populate_cluster(keys_num, size)
    self.loop_size = keys_num * size
    self.counter = 0

    EM.synchrony do
      measure_time(Time.now, "Populating of #{keys_num} keys with #{size} members took")

      EM::Synchrony::FiberIterator.new(keys_num.times.to_a, 2000).each do |i|
        self.populate_keys(i, size)
      end

    end
  end

end


redis_hosts = ["redis://redis-host2.com:6379/1", "redis://redis-host3.com:6379/1"]

mb = MigratorBenchmark.new(redis_hosts)

mb.populate_cluster(1000, 100)
