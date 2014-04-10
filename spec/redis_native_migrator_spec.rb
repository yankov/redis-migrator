require_relative 'shared_hosts_context'
require_relative 'different_redis_type_migrator'
require_relative 'pretested_migrator'

describe Redis::NativeMigrator do
  let(:migrator) { Redis::NativeMigrator.new(old_hosts) }
  include_context 'shared hosts context'

  let(:old_cluster) { Redis::Distributed.new(old_hosts) }
  let(:new_cluster) { Redis::Distributed.new(new_hosts) }

  before { allow(migrator).to receive(:redis).and_return(old_cluster) }

  describe '#migrate' do
    context do
      let(:node) { new_cluster.node_for(key) }
      let(:source_node) { old_cluster.node_for(key) }
      let(:destination_cluster) { source_node.client.select(10); source_node }
      before do
        allow_any_instance_of(MockRedis).to receive(:migrate) do |key|
          source_node.move(key, 10)
        end
      end

      it_behaves_like 'different redis type migrator'
    end

    context do
      let(:old_cluster) { Redis::Distributed.new([old_hosts.first]) }
      let(:source_node) { old_cluster.nodes.first }
      let(:destination_cluster) { source_node.client.select(10); source_node }

      before do
        allow_any_instance_of(MockRedis).to receive(:migrate) do |key|
          source_node.move(key, 10)
        end
      end

      it_behaves_like 'pretested migrator'
      # Not supported in Redis < 3.0
      # it_behaves_like 'safe pretested migrator'
    end
  end
end
