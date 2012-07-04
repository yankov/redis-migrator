# require './migrator.rb'
require 'rubygems'
require 'digest'
require 'redis'
require 'redis/distributed'
require 'uri'
require 'benchmark'
require_relative 'lib/redis_populator'


# redis_hosts = ["redis://redis-host1.com:6379/1", "redis://redis-host2.com:6379/1"]

redis_hosts = ["redis://localhost:6379/1", "redis://localhost:6378/1"]
r = Redis::Populator.new(redis_hosts)

# migrator = Redis::Migrator.new(["redis-host1.com:6379", "redis-host2.com:6379"],
#                                ["redis-host1.com:6379", "redis-host2.com:6379", "redis-host3.com:6379"])


# migrator = Redis::Migrator.new(["localhost:6379/1", "localhost:6378/1"],
#                                ["localhost:6379/1", "localhost:6378/1", "localhost:6377/1"])


Benchmark.bm do |x|
  x.report("populate:") { r.populate_cluster(1000, 100) }
  # x.report("migarate:") { migrator.migrate_cluster }
end

