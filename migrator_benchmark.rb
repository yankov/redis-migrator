require 'migrator.rb'
require 'benchmark'
require "ruby-debug"
require 'celluloid'

class MigratorBenchmark
  include Celluloid

  def initialize(old_hosts, new_hosts)
    @old_hosts = old_hosts.map{|h| "redis://" + h}
    @new_hosts = old_hosts.map{|h| "redis://" + h}
  end

  def populate_keys(redis, i, num)
    begin 
      key = ::Digest::MD5.hexdigest(i.to_s)

      num.times do |x|
        redis.sadd(key, ::Digest::MD5.hexdigest("f" + x.to_s))
      end
    rescue => e
      p e.message
    end
  end

  def populate_cluster(keys_num, size)
    pool = MigratorBenchmark.pool(size: 400, args: [@old_hosts, @new_hosts])

    keys_num.times do |i|
      pool.future(:populate_keys, Redis::Distributed.new(@old_hosts), i, size)
    end
  end

end


bc = MigratorBenchmark.new(["redis-host1.com:6379/1", "redis-host2.com:6379/1"],
                           ["redis-host1.com:6379/1", "redis-host2.com:6379/1", "redis-host3.com:6379/1"])

r = Redis::Distributed.new(["redis://redis-host1.com:6379/1", "redis://redis-host2.com:6379/1"])
r.flushdb

bc.populate_cluster(400, 100) 

