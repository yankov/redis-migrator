require 'redis_migrator'
require 'uri'
require 'benchmark'
require 'redis_migrator/redis_populator'

# a list of hosts for an old cluster
# You either have to start 3 Redis instances on your local - each on its
# own port: 6379, 6378, 6377. Or, better, have a real Redis nodes running
old_redis_hosts = ["redis://localhost:6379/9", "redis://localhost:6378/9"]

# a list of hosts for a new cluster
new_redis_hosts = ["redis://localhost:6379/9", "redis://localhost:6378/9", "redis://localhost:6377/9"]

r = Redis::Populator.new(old_redis_hosts)

migrator = Redis::Migrator.new(old_redis_hosts, new_redis_hosts)                               

Benchmark.bm do |x|
  x.report("populate:") { r.populate_cluster(1000, 100) }
  x.report("migrate:")  { migrator.run }
end