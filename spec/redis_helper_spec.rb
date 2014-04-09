describe Redis::Helper do
  let(:migrator) do
    Redis::Migrator.new(['redis://localhost:6379'], ['redis://localhost:6377'])
  end

  let(:old_cluster) { migrator.old_cluster }
  let(:new_cluster) { migrator.new_cluster }
  let(:pipe) { PipeMock.new(new_cluster) }

  before do
    expect(Redis).to receive(:new).at_least(1).times do |options|
      MockRedis.new(options)
    end

    allow(migrator).to receive(:redis).and_return(old_cluster)
  end

  it 'should copy a string' do
    old_cluster.set('a', 'some_string')
    migrator.copy_string(pipe, 'a')

    new_cluster.get('a').should == 'some_string'
  end

  it 'should copy a hash' do
    old_cluster.hmset('myhash',
      'first_name', 'James',
      'last_name', 'Randi',
      'age', '83')

    migrator.copy_hash(pipe, 'myhash')

    new_cluster.hgetall('myhash').should == {'first_name' => 'James',
                                             'last_name' => 'Randi',
                                             'age' => '83'}
  end

  it 'should copy a list' do
    ('a'..'z').to_a.each { |val| old_cluster.lpush('mylist', val) }
    migrator.copy_list(pipe, 'mylist')

    new_cluster.lrange('mylist', 0, -1).should == ('a'..'z').to_a
  end

  it 'should copy a set' do
    ('a'..'z').to_a.each { |val| old_cluster.sadd('myset', val) }
    migrator.copy_set(pipe, 'myset')

    new_cluster.smembers('myset').should == ('a'..'z').to_a
  end

  it 'should copy zset' do
    ('a'..'z').to_a.each { |val| old_cluster.zadd('myzset', rand(100), val) }
    migrator.copy_zset(pipe, 'myzset')

    new_range = new_cluster.zrange('myzset', 0, -1, with_scores: true).sort
    old_range = old_cluster.zrange('myzset', 0, -1, with_scores: true).sort

    new_range.should == old_range
  end
end
