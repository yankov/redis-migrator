require './migrator.rb'
require 'rubygems'
require 'uri'
require 'benchmark'
require_relative 'lib/redis_populator'

# old_redis_hosts = ["redis://redis-host1.com:6379/1", "redis://redis-host2.com:6379/1"]
# new_redis_hosts = ["redis://redis-host1.com:6379/1", "redis://redis-host2.com:6379/1", "redis://redis-host3.com:6379/1"]

old_redis_hosts = ["redis://localhost:6379/1", "redis://localhost:6378/1"]
new_redis_hosts = ["redis://localhost:6379/1", "redis://localhost:6378/1", "redis://localhost:6377/1"]

r = Redis::Populator.new(old_redis_hosts)

migrator = Redis::Migrator.new(old_redis_hosts, new_redis_hosts)                               

Benchmark.bm do |x|
  x.report("populate:") { r.populate_cluster(1000, 100) }
  x.report("migrate:")  { migrator.migrate_cluster }
end

