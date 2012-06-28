require 'migrator.rb'
require 'celluloid'
require 'digest'

class MigratorBenchmark
  include Celluloid

  def populate_keys(redis, i, num)
    key = ::Digest::MD5.hexdigest(i.to_s)

    num.times do |x|
      redis.sadd(key, ::Digest::MD5.hexdigest("f" + x.to_s))
    end
  rescue => e
    p e.message
  end

end

class Populator

  def self.populate_cluster(redis_hosts, keys_num, size)
    pool = MigratorBenchmark.pool(:size => 400)

    keys_num.times do |i|
      pool.future(:populate_keys, Redis::Distributed.new(redis_hosts), i, size)
    end
  end

end

redis_hosts = ["redis://redis-host1.com:6379/1", "redis://redis-host2.com:6379/1"]

# just to flush redis
Redis::Distributed.new(redis_hosts).flushdb

Populator.populate_cluster(redis_hosts, 400, 100)




