require_relative 'shared_hosts_context'
require_relative 'different_redis_type_migrator'
require_relative 'pretested_migrator'

describe Redis::PipeMigrator do
  let(:migrator) { Redis::PipeMigrator.new(old_hosts) }
  include_context 'shared hosts context'

  let(:old_cluster) { Redis::Distributed.new(old_hosts) }
  let(:new_cluster) { Redis::Distributed.new(new_hosts) }
  let(:pipe) { PipeMock.new(new_cluster) }

  before { allow(migrator).to receive(:redis).and_return(old_cluster) }

  describe '#migrate' do
    context do
      before { expect(IO).to receive(:popen).and_return(pipe) }
      it_behaves_like 'different redis type migrator'
    end

    context do
      before do
        command = 'redis-cli -h localhost -p 6378 -n 1 --pipe'
        expect(IO).to receive(:popen).with(command, IO::RDWR).and_return(pipe)
      end

      it_behaves_like 'pretested migrator'
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
      before { old_cluster.sadd('a', 1) }

      it 'calls copy_set' do
        expect(migrator).to receive(:copy_set).with(nil, 'a')
        subject
      end
    end
  end
end
