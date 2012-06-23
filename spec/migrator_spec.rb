describe Redis::Migrator do
  
  before do
    Redis.should_receive(:new).any_number_of_times {|options|  
      MockRedis.new(options) 
    }
  
    @migrator = Redis::Migrator.new(["localhost:6379", "localhost:6378"],
                 ["localhost:6379", "localhost:6378", "localhost:6377"])

    @migrator.populate_keys(('a'..'z').to_a)
  end

  it "should show keys which need migration" do
    @migrator.changed_keys.sort.should == ["h", "j", "m", "n", "o", "q", "s", "y"]
  end

  it "should migrate given keys to a new cluster" do
    keys = ["q", "s", "j"]

    @migrator.migrate_keys(keys)

    (@migrator.new_cluster.keys("*") & keys).sort.should == ["j", "q", "s"]
    (@migrator.old_cluster.keys("*") & keys).sort.should == []
  end

  it "should migrate all keys for which nodes have changed" do
    @migrator.migrate_cluster

    (@migrator.old_cluster.keys("*") & @migrator.changed_keys).should == []
    (@migrator.new_cluster.keys("*") & @migrator.changed_keys).should == @migrator.changed_keys
  end
  
end