describe Redis::Helper do 
  before do
    Redis.should_receive(:new).any_number_of_times {|options|  
      MockRedis.new(options) 
    }

    @migrator = Redis::Migrator.new(["redis://localhost:6379"],
                                    ["redis://localhost:6377"])

    @r1 = @migrator.old_cluster
    @r2 = @migrator.new_cluster
    @migrator.stub!(:redis).and_return(@r1)
    @pipe = PipeMock.new(@r2)
  end

  it "should copy a string" do
    @r1.set("a", "some_string")
    @migrator.copy_string(@pipe, "a")

    @r2.get("a").should == "some_string"
  end

  it "should copy a hash" do
    @r1.hmset("myhash", 
      "first_name", "James",
      "last_name", "Randi",
      "age", "83")

    @migrator.copy_hash(@pipe, "myhash")

    @r2.hgetall("myhash").should == {"first_name" => "James", "last_name" => "Randi", "age" => "83"}
  end

  it "should copy a list" do
    ('a'..'z').to_a.each { |val| @r1.lpush("mylist", val) }

    @migrator.copy_list(@pipe, "mylist")

    @r2.lrange("mylist", 0, -1).should == ('a'..'z').to_a
  end

  it "should copy a set" do
    ('a'..'z').to_a.each { |val| @r1.sadd("myset", val) } 

    @migrator.copy_set(@pipe, "myset")

    @r2.smembers("myset").should == ('a'..'z').to_a
  end

  it "should copy zset" do
    ('a'..'z').to_a.each { |val| @r1.zadd("myzset", rand(100), val) } 

    @migrator.copy_zset(@pipe, "myzset")

    @r2.zrange("myzset", 0, -1, :with_scores => true).sort.should == @r1.zrange("myzset", 0, -1, :with_scores => true).sort
  end

end