describe Redis::Migrator do
  let(:migrator) { Redis::Migrator.new(old_hosts, new_hosts) }
  let(:old_hosts) { %w(redis://localhost:6379 redis://localhost:6378) }
  let(:new_hosts) { old_hosts + ['redis://localhost:6377'] }

  before do
    expect(Redis).to receive(:new).at_least(1).times do |options|
      MockRedis.new(options) 
    end
  
    #populate old cluster with some keys
    ('a'..'z').to_a.each do |key|
      (1..5).to_a.each {|val| migrator.old_cluster.sadd(key, val)}
    end
  end

  describe '#changed_keys' do
    it 'should show keys which need migration' do
      migrator.changed_keys.should == {'redis://localhost:6377/0' => %w(h q s y j m n o)}
    end
  end

  describe '#migrate_keys' do
    let(:keys) { %w(q s j) }
    let(:node) { { host: 'localhost', port: 6378, db: 1 } }
    let(:pipe) { PipeMock.new(migrator.new_cluster) }
    let(:options) { {} }

    before do
      expect(Redis::Distributed).to receive(:new).and_return(migrator.old_cluster)

      command = 'redis-cli -h localhost -p 6378 -n 1 --pipe'
      expect(IO).to receive(:popen).with(command, IO::RDWR).and_return(pipe)
      migrator.migrate_keys(node, keys, options)
    end

    def common_keys(cluster)
      (cluster.keys('*') & keys).sort
    end

    subject { common_keys(cluster) }

    context do
      let(:cluster) { migrator.new_cluster }
      it 'should copy given keys to a new cluster' do
        should == %w(j q s)
      end
    end

    context do
      let(:cluster) { migrator.old_cluster }

      it 'should remove copied keys from the old redis node' do
        should == []
      end

      context 'when asked to not remove' do
        let(:options) { { do_not_remove: true } }
        it 'should keep keys on old node' do
          should == ["j", "q", "s"]
        end
      end
    end
  end

  describe '#copy_key' do
    subject { migrator.copy_key(nil, key) }

    context 'with unknown key' do
      let(:key) { 'some_key' }
      it { should == false }
    end

    context 'when known set key' do
      let(:key) { 'a' }

      it 'calls copy_set' do
        expect(migrator).to receive(:copy_set).with(nil, 'a')
        subject
      end
    end
  end
end
