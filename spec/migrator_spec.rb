describe Redis::Migrator do
  
  before do
    Redis.should_receive(:new).any_number_of_times {|options|  
      MockRedis.new(options) 
    }
  
    @migrator = Redis::Migrator.new(["redis://localhost:6379", "redis://localhost:6378"],
                                    ["redis://localhost:6379", "redis://localhost:6378", "redis://localhost:6377"])

    #populate old cluster with some keys
    ('a'..'z').to_a.each do |key|
      (1..5).to_a.each {|val| @migrator.old_cluster.sadd(key, val)}
    end
  end

  describe Redis::Migrator, "#changed_keys" do
    it "should show keys which need migration" do
      @migrator.changed_keys.should == {"redis://localhost:6377/0" => ["h", "q", "s", "y", "j", "m", "n", "o"]}
    end
  end

  describe Redis::Migrator, "#migrate_keys" do
    let(:keys) { ["q", "s", "j"] }
    let(:node) { {:host => "localhost", :port => 6378, :db => 1} }

    before do
      Redis::Distributed.should_receive(:new).and_return(@migrator.old_cluster)
      
      @pipe = PipeMock.new(@migrator.new_cluster)
      IO.should_receive(:popen).with("redis-cli -h localhost -p 6378 -n 1 --pipe", IO::RDWR).and_return(@pipe)
    end

    it "should copy given keys to a new cluster" do
      @migrator.migrate_keys(node, keys)
      (@migrator.new_cluster.keys("*") & keys).sort.should == ["j", "q", "s"]
    end

    it "should remove copied keys from the old redis node" do
      @migrator.migrate_keys(node, keys)
      (@migrator.old_cluster.keys("*") & keys).sort.should == []      
    end

    it "should keep keys on old node if asked" do
      @migrator.migrate_keys(node, keys, :do_not_remove => true)
      (@migrator.old_cluster.keys("*") & keys).sort.should == ["j", "q", "s"]      
    end
  end

  describe Redis::Migrator, "#copy_key" do

    it "should return FALSE for unknown key" do
      @migrator.copy_key(nil, "some_key").should == false
    end

    it "should call copy_set if given key is set" do
      @migrator.should_receive(:copy_set).with(nil, "a")
      @migrator.copy_key(nil, "a")
    end

  end

end
