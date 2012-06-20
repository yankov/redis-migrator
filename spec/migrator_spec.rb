describe Migrator do

    let!(:migrator) { 
    Migrator.new(["localhost:6379", "localhost:6378"],
                 ["localhost:6379", "localhost:6378", "localhost:6377"])
  }

  
  describe Migrator, ".changed_keys" do

    it "should show keys which need migration" do

      ('a'..'z').to_a.each do |key|
        (1..10).to_a.each do |val|
          migrator.old_cluster.sadd(key, val)
        end
      end

      migrator.changed_keys.should == ["q", "s", "h", "y", "m", "n", "o", "j"]

    end

  end

  describe Migrator, ".migrate_keys" do
    it "should migrate given keys to a new cluster" do
      keys = ["q", "s", "j"]
      migrator.migrate_keys(keys)

      (migrator.new_cluster.keys("*") & keys).should == ["s", "q", "j"]
      (migrator.old_cluster.keys("*") & keys).should == []
    end

  end

  describe Migrator, ".migrate_cluster" do
    it "should migrate all keys for which nodes have changed" do
      (migrator.old_cluster.keys("*") & migrator.changed_keys).should == migrator.changed_keys
  
      migrator.migrate_cluster

      (migrator.old_cluster.keys("*") & migrator.changed_keys).should == []
      (migrator.new_cluster.keys("*") & migrator.changed_keys).should == migrator.changed_keys
      
    end
  end
  
end