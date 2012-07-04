# require './migrator.rb'
require 'rubygems'
require 'digest'
require 'redis'
require 'redis/distributed'
require 'uri'
require 'benchmark'

class MigratorBenchmark

  def initialize(redis_hosts)
    @redis_hosts = redis_hosts 
  end

  def redis
    @redis ||=  Redis::Distributed.new(@redis_hosts)
  end

  def parse_redis_url(redis_url)
    node = URI(redis_url)
    path = node.path
    db = path[1..-1].to_i rescue 0

    {
      :host => node.host,
      :port => node.port,
      :db   => db
    }
  end

  # generate sets' keys to populate redis cluster
  # @param num size [Integer] the number of sets that have to be created
  # @return hash of keys grouped by redis node
  def generate_keys(num)
    num.times.inject({}) do |acc, i| 
      key = ::Digest::MD5.hexdigest(i.to_s)
      node = redis.node_for(key).client
      hash_key = "redis://#{node.host}:#{node.port}/#{node.db}"
      acc[hash_key] = [] if acc[hash_key].nil?
      acc[hash_key] << key
      acc
    end
  end

  # populates sets with the given amount of members
  # @param node [Hash] a parsed redis_url 
  # @param keys [Array] an array of sets' keys that need to be populated 
  def populate_keys(node, keys, size)
    f = IO.popen("redis-cli -h #{node[:host]} -p #{node[:port]} -n #{node[:db]} --pipe", IO::RDWR)

    keys.each do |key|
      size.times.map do |x| 
        f << to_redis_proto(*["SADD", key, ::Digest::MD5.hexdigest("f" + x.to_s)])
      end
    end

    f.close
  end

  # populates redis cluster 
  # @param keys_num [Integer] amount of redis sets that need to be populated
  # @param num [Integer] number of members in each set
  def populate_cluster(keys_num, num)
    redis.flushdb

    nodes = generate_keys(keys_num)
    threads = []

    nodes.keys.each do |node_url|
      node = parse_redis_url(node_url)

      threads << Thread.new(node, nodes[node_url], num) {|node, keys, size|
        populate_keys(node, keys, size)
      }
    end

    threads.each{|t| t.join}
  end

end

def to_redis_proto(*cmd)
  cmd.inject("*#{cmd.length}\r\n") {|acc, arg|
    acc << "$#{arg.length}\r\n#{arg}\r\n"
  }
end


redis_hosts = ["redis://redis-host1.com:6379/1", "redis://redis-host2.com:6379/1"]

# redis_hosts = ["redis://localhost:6379/1", "redis://localhost:6378/1"]
mb = MigratorBenchmark.new(redis_hosts)

# migrator = Redis::Migrator.new(["redis-host1.com:6379", "redis-host2.com:6379"],
#                                ["redis-host1.com:6379", "redis-host2.com:6379", "redis-host3.com:6379"])


# migrator = Redis::Migrator.new(["localhost:6379/1", "localhost:6378/1"],
#                                ["localhost:6379/1", "localhost:6378/1", "localhost:6377/1"])


Benchmark.bm do |x|
  x.report("populate:") { mb.populate_cluster(10000, 100) }
  # x.report("migarate:") { migrator.migrate_cluster }
end

