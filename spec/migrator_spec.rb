require "/Users/yankov/projects/mock_redis/lib/mock_redis.rb"

class MockRedis
  class Database

    def initialize(*args)
      @data = {}
      @expire_times = []
      @client = args.last
    end

    def client
      @client
    end

  end
end

class Redis
  class Distributed

    def initialize(urls, options = {})
      @tag = options.delete(:tag) || /^\{(.+?)\}/
      @default_options = options
      @ring = HashRing.new urls.map { |url| MockRedis.new(options.merge(:url => url)) }
      @subscribed_node = nil
    end

    def add_node(url)
      @ring.add_node MockRedis.new(@default_options.merge(:url => url))
    end

  end
end

describe Migrator do

    before(:all) do

      @migrator = Migrator.new(["localhost:6379", "localhost:6378"],
                 ["localhost:6379", "localhost:6378", "localhost:6377"])
    end

  describe Migrator, ".changed_keys" do

    it "should show keys which need migration" do

      ('a'..'z').to_a.each do |key|
        (1..10).to_a.each do |val|
          @migrator.old_cluster.sadd(key, val)
        end
      end

      @migrator.changed_keys.should == ["h", "q", "s", "y", "j", "m", "n", "o"]
    end

  end

  describe Migrator, ".migrate_keys" do
    it "should migrate given keys to a new cluster" do
      keys = ["q", "s", "j"]

      @migrator.migrate_keys(keys)
      (@migrator.new_cluster.keys("*") & keys).should == ["q", "s", "j"]
      (@migrator.old_cluster.keys("*") & keys).should == []
    end

  end

  describe Migrator, ".migrate_cluster" do
    it "should migrate all keys for which nodes have changed" do
      (@migrator.old_cluster.keys("*") & @migrator.changed_keys).should == @migrator.changed_keys
  
      @migrator.migrate_cluster

      (@migrator.old_cluster.keys("*") & @migrator.changed_keys).should == []
      (@migrator.new_cluster.keys("*") & @migrator.changed_keys).should == @migrator.changed_keys
      
    end
  end
  
end