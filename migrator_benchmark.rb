require './migrator.rb'
require 'digest'
require 'em-synchrony'
require "em-synchrony/fiber_iterator"
require 'em-hiredis'
require 'hiredis'
require 'redis/distributed'
require './lib/support/hiredis.rb'
require './lib/support/redis_distributed.rb'

class MigratorBenchmark

  attr_accessor :redis, :counter, :loop_size 

  def initialize(redis_hosts)
    @redis_hosts = redis_hosts 
  end

  def redis
    if EM.reactor_running?
      @em_redis ||= Redis::Distributed.new(@redis_hosts, :em => true)
    else
      @redis ||=  Redis::Distributed.new(@redis_hosts)
    end
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
    
    commands = num.times.map do |x| 
      ["sadd", key, ::Digest::MD5.hexdigest("f" + x.to_s)]
    end
    
    redis.node_for(key).client.call_pipelined(commands)    
  end



  def populate_cluster(keys_num, size)
    redis.flushdb


    nodes = keys_num.times.to_a.inject({}) do |a, i|
      # self.populate_keys(i, size)
      key = ::Digest::MD5.hexdigest(i.to_s)

      a[redis.node_for(key).id] = [] if a[redis.node_for(key).id].nil?
      a[redis.node_for(key).id] << key
      a
    end

    nodes.each do |node, keys|

      commands = keys.inject([]) do |acc, key| 
        size.times.to_a.each do |x|
          acc << ["sadd", key, ::Digest::MD5.hexdigest("f" + x.to_s)]
          if acc.count == 1000
            redis.node_for(keys.first).client.call_pipelined(acc)
            acc = []
          end
        end

        acc
      end

      p "populating #{node}"
      redis.node_for(keys.first).client.call_pipelined(commands) unless commands.empty?

    end

  end

end

def without_gc
  GC.start
  GC.disable
  yield
ensure
  GC.enable
end


# redis_hosts = ["redis://redis-host2.com:6379/1", "redis://redis-host3.com:6379/1"]

redis_hosts = ["redis://localhost:6379/1", "redis://localhost:6378/1"]
mb = MigratorBenchmark.new(redis_hosts)

start_time = Time.now

x = without_gc {
 mb.populate_cluster(10000, 1000)
}

puts "Took #{Time.now - start_time} seconds"


# migrator = Redis::Migrator.new(["redis-host1.com:6379", "redis-host2.com:6379"],
#                                ["redis-host1.com:6379", "redis-host2.com:6379", "redis-host3.com:6379"])


# migrator = Redis::Migrator.new(["localhost:6379/1", "localhost:6378/1"],
#                                ["localhost:6379/1", "localhost:6378/1", "localhost:6377/1"])


# migrator.migrate_cluster

