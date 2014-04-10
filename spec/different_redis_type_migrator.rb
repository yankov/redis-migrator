shared_examples 'different redis type migrator' do
  context 'key of type' do
    let(:keys) { [key] }

    subject { migrator.migrate(node, keys, {}) }

    context 'string' do
      let(:key) { 'a' }

      it 'should copy' do
        old_cluster.set(key, 'some_string')
        subject
        destination_cluster.get(key).should == 'some_string'
      end
    end


    context 'hash' do
      let(:key) { 'myhash' }

      it 'should copy' do
        old_cluster.hmset(key,
                          'first_name', 'James',
                          'last_name', 'Randi',
                          'age', '83')
        subject
        destination_cluster.hgetall(key).should == {'first_name' => 'James',
                                                    'last_name' => 'Randi',
                                                    'age' => '83'}
      end
    end

    context 'list' do
      let(:key) { 'mylist' }

      it 'should copy' do
        ('a'..'z').to_a.each { |val| old_cluster.lpush(key, val) }
        values = old_cluster.lrange(key, 0, -1)
        subject
        destination_cluster.lrange(key, 0, -1).should == values
      end
    end

    context 'set' do
      let(:key) { 'myset' }
      it 'should copy' do
        ('a'..'z').to_a.each { |val| old_cluster.sadd(key, val) }
        values = old_cluster.smembers(key)
        subject
        destination_cluster.smembers(key).should == values
      end
    end

    context 'zset' do
      let(:key) { 'myzset' }
      it 'should copy' do
        ('a'..'z').to_a.each { |val| old_cluster.zadd(key, rand(100), val) }
        old_range = old_cluster.zrange(key, 0, -1, with_scores: true).sort

        subject

        new_range = destination_cluster.zrange(key, 0, -1, with_scores: true).sort
        new_range.should == old_range
      end
    end
  end
end
