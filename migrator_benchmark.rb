require 'migrator.rb'
require 'benchmark'
require "ruby-debug"

class MigratorBenchmark

  # populate cluster => measure time 
  # get changed keys => measure time
  # migrate cluster  => measure time

  attr_reader :migrator

  def initialize(old_hosts, new_hosts)
    @old_hosts = old_hosts.map{|h| "redis://" + h}
    @migrator = Redis::Migrator.new(old_hosts, new_hosts) 
  end

  def populate_cluster(keys_num, size)
    thread_pool = []
    keys_num.times do |i|
      value = ::Digest::MD5.hexdigest(i.to_s)
      thread_pool << Thread.new(Redis::Distributed.new(@old_hosts), value, size) do |redis, k, num|
        begin 
          num.times do |x|
            redis.sadd(k, ::Digest::MD5.hexdigest("f" + x.to_s))
          end
        rescue => e
          p e.message
        end
      end
    end

    thread_pool.each {|th| th.join}      

  end

end


bc = MigratorBenchmark.new(["localhost:6379", "localhost:6378"],
                           ["localhost:6379", "localhost:6378", "localhost:6377"])

bc.migrator.new_cluster.flushdb

Benchmark.bm do |x|
  x.report { bc.populate_cluster(100, 1000) } 
end