require_relative 'shared_hosts_context'

describe Redis::Migrator do
  include_context 'shared hosts context'
  let(:migrator) { Redis::Migrator.new(old_hosts, new_hosts) }
  
  describe '#fetch_keys_from_redis' do
    let(:node) { migrator.old_cluster.nodes.first }
    
    it "should work with lower Redis version" do
      allow(node).to(
        receive(:keys).and_return(%W(h q s y j m n o))
      )
      allow(node).to(
        receive(:info).and_return({ "redis_version" => "2.7.9" })
      )
      expect(migrator.fetch_keys_from_redis(node, 0)).to eq ["0", ["h", "q", "s", "y", "j", "m", "n", "o"]]
    end
    
    it 'should use scan method for Redis 2.8.0+' do
      allow(node).to(
        receive(:scan).and_return(["0",%W(h q s y j m n o)])
      )
      allow(node).to(
        receive(:info).and_return({ "redis_version" => "2.8.0" })
      )
      expect(migrator.fetch_keys_from_redis(node, 0)).to eq ["0", ["h", "q", "s", "y", "j", "m", "n", "o"]]
    end
  end

  describe '#scan_keys' do
    before { prefill_cluster(migrator.old_cluster) }

    it 'should show keys which need migration' do
      allow(migrator).to(
        receive(:fetch_keys_from_redis).and_return(["0",%W(h q s y j m n o)])
      )
      migrator.scan_keys do |result, size|
        # There have duplicate keys, because it had 2 redis instance
        result.should == {'redis://localhost:6377/0' => %w(h q s y j m n o h q s y j m n o)}
        size.should == 16
      end
    end
  end

  describe '#migrator' do
    subject { migrator.send(:migrator, keep_original) }
    let(:keep_original) { false }

    before do
      allow_any_instance_of(MockRedis).to(
          receive(:info).and_return({ 'redis_version' => version })
      )
    end

    context 'when all instances are old' do
      let(:version) { '2.4.1' }
      it { should == Redis::PipeMigrator}
    end

    context 'when all instances are new' do
      let(:version) { '2.6.14' }
      it { should == Redis::NativeMigrator }

      context 'when asking to preserve data on source' do
        let(:keep_original) { true }

        it { should == Redis::PipeMigrator }
      end
    end
  end
end
