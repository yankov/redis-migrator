require 'rubygems'
require 'redis'
require 'redis/distributed'
require 'md5'

class Redis
  class Migrator

    attr_accessor :old_cluster, :new_cluster

    def initialize(old_hosts, new_hosts)
      @old_hosts   = old_hosts.map{|h| "redis://" + h}
      @new_hosts   = new_hosts.map{|h| "redis://" + h}

      @old_cluster = Redis::Distributed.new(@old_hosts)
      @new_cluster = Redis::Distributed.new(@new_hosts)
    end

    def populate_cluster(keys_num, followers_size)
      thread_pool = []

      keys_num.times do |i|
        id = "player:" + ::Digest::MD5.hexdigest(i.to_s) + ":followers"

        thread_pool << Thread.new(Redis::Distributed.new(@old_hosts), id, followers_size) do |redis, k, num|
          begin 
            num.times do |x|
              follower_id = ::Digest::MD5.hexdigest("f" + x.to_s)
              redis.sadd(k, follower_id)
            end
          rescue => e
            p e.message
          end
        
        end

      end

      thread_pool.each {|th| th.join}      
    end

    def populate_keys(keys)
      keys.each do |key|
        (1..100).to_a.each do |val|
          old_cluster.sadd(key, val)
        end
      end
    end

    def changed_keys
      keys = old_cluster.keys("*")

      changed_keys = keys.inject([]) do |acc, key|
        old_client = old_cluster.node_for(key).client
        new_client = new_cluster.node_for(key).client

        acc << key if "#{old_client.host}:#{old_client.port}" != "#{new_client.host}:#{new_client.port}"
        acc
      end
    end

    def migrate_keys(keys)
      return false if keys.empty? || keys.nil?

      keys.each do |key|
        #rewrite key members to the new node
        old_cluster.smembers(key).each do |member|
          new_cluster.sadd(key, member)
        end

        #remove key from the old node
        old_cluster.node_for(key).del(key)
      end
      
    end

    def migrate_cluster(options={})
      options[:threads_num] ||= 1

      migrating_keys = self.changed_keys

      migrating_keys.each_slice(options[:threads_num]) do |keys_slice| 

        thread_pool = []

        thread_pool << Thread.new do
          migrate_keys(keys_slice)
        end
        
        thread_pool.each {|th| th.join}
      end

    end

  end # class Migrator
end # class Redis
