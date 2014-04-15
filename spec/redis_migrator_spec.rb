require_relative 'shared_hosts_context'

describe Redis::Migrator do
  include_context 'shared hosts context'
  let(:migrator) { Redis::Migrator.new(old_hosts, new_hosts) }

  describe '#scan_keys' do
    before { prefill_cluster(migrator.old_cluster) }

    it 'should show keys which need migration' do
      allow_any_instance_of(MockRedis).to(
        receive(:scan).and_return(["0",%W(h q s y j m n o)])
      )
      allow_any_instance_of(MockRedis).to(
        receive(:info).and_return({ "redis_version" => "2.8.0" })
      )
      migrator.scan_keys do |result, size|
        # There have duplicate keys, because it had 2 redis instance
        result.should == {'redis://localhost:6377/0' => %w(h q s y j m n o h q s y j m n o)}
        size.should == 16
      end
    end
    
    it "should run on lower Redis version" do
      allow_any_instance_of(MockRedis).to(
        receive(:keys).and_return(%W(h q s y j m n o))
      )
      allow_any_instance_of(MockRedis).to(
        receive(:info).and_return({ "redis_version" => "2.7.9" })
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
