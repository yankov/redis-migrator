require 'em-synchrony'
require "em-synchrony/fiber_iterator"
require 'em-hiredis'
require 'redis/distributed'
require './lib/support/hiredis.rb'
require './lib/support/redis_distributed.rb'
require 'ruby-debug'

class Redis
  class Migrator

    def initialize(old_hosts, new_hosts)
      @old_hosts   = old_hosts.map{|h| "redis://" + h}
      @new_hosts   = new_hosts.map{|h| "redis://" + h}
    end

    class << self
      def copy_string(r1, r2, key)
        value = r1.get(key).callback {|value|
          r2.set(key, value)
        }
      end
    
      def copy_hash(r1, r2, key)
        r1.hgetall(key).each do |field, value|
          r2.hset(key, field, value)
        end
      end
    
      def copy_list(r1, r2, key)
        r1.lrange(key, 0, -1).each do |value|
          r2.lpush(key, value)
        end
      end
    
      def copy_set(r1, r2, key)
        r1.smembers(key).callback do |members|
          size = members.count
          counter = 0

          members.each { |member| r2.sadd(key, member).callback { 
            counter += 1
            if counter == size
              old_port = r1.node_for(key).port
              r1.node_for(key).del(key)
            end
            } 
          }
        end
      end
    
      def copy_zset(r1, r2, key)
        r1.zrange(key, 0, -1, :with_scores => true).each_slice(2) do |member, score|
          r2.zadd(key, score, member)
        end 
      end
    end # class << self

    def old_cluster
      if EM.reactor_running?
        @em_old_cluster ||= Redis::Distributed.new(@old_hosts, :em => true)
      else
        @not_em_old_cluster ||=  Redis::Distributed.new(@old_hosts)
      end
    end

    def new_cluster
      if EM.reactor_running?
        @em_new_cluster ||= Redis::Distributed.new(@new_hosts, :em => true)
      else
        @not_em_new_cluster ||=  Redis::Distributed.new(@new_hosts)
      end
    end

    def stop_when(&blk)
      EM::add_periodic_timer( 2 ) do 
        next unless blk.call
        EM.stop
      end
    end

    def changed_keys
      keys = old_cluster.keys("*")

      keys.inject([]) do |acc, key|
        old_client = old_cluster.node_for(key).client
        new_client = new_cluster.node_for(key).client

        acc << key if "#{old_client.host}:#{old_client.port}" != "#{new_client.host}:#{new_client.port}"
        
        acc
      end

    end

    def migrate_keys(keys)
      return false if keys.empty? || keys.nil?

      EM.synchrony do
        EM::Synchrony::FiberIterator.new(keys, 2000).each {|key|
          copy_key(old_cluster, new_cluster, key)
        }
      end
    end

    def migrate_cluster(options={})
      puts "Migrating #{self.changed_keys.count} keys"

      migrate_keys(self.changed_keys)
    end

    def copy_key(old_cluster, new_cluster, key)
      old_cluster.type(key).callback {|key_type|
        next unless ['list', 'hash', 'string', 'set', 'zset'].include?(key_type)

        Migrator.send("copy_#{key_type}", old_cluster, new_cluster, key)
      }
    end

  end # class Migrator
end # class Redis
