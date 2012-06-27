require 'migrator.rb'
require 'benchmark'
require "ruby-debug"
require 'celluloid'

class MigratorBenchmark
  include Celluloid

  attr_reader :migrator

  def initialize(old_hosts, new_hosts)
    @old_hosts = old_hosts.map{|h| "redis://" + h}
    @new_hosts = old_hosts.map{|h| "redis://" + h}

    @migrator = Redis::Migrator.new(old_hosts, new_hosts) 
  end

  def populate_keys(redis, key, num)
    begin 
      num.times do |x|
        redis.sadd(key, ::Digest::MD5.hexdigest("f" + x.to_s))
      end
    rescue => e
      p e.message
    end
  end

  def populate_cluster(keys_num, size)
    pool = MigratorBenchmark.pool(size: 500, args: [@old_hosts, @new_hosts])

    keys_num.times do |i|
      value = ::Digest::MD5.hexdigest(i.to_s)

      pool.populate_keys!(Redis::Distributed.new(@old_hosts), value, size)
    end

  end

end


bc = MigratorBenchmark.new(["redis-host1.com:6379/1", "redis-host2.com:6379/1"],
                           ["redis-host1.com:6379/1", "redis-host2.com:6379/1", "redis-host3.com:6379/1"])

bc.migrator.new_cluster.flushdb

Benchmark.bm do |x|
  x.report { bc.populate_cluster(1000, 1000) } 
end