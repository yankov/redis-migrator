require_relative 'shared_hosts_context'

describe Redis::Migrator do
  include_context 'shared hosts context'
  let(:migrator) { Redis::Migrator.new(old_hosts, new_hosts) }

  describe '#changed_keys' do
    before { prefill_cluster(migrator.old_cluster) }

    it 'should show keys which need migration' do
      migrator.changed_keys.should == {'redis://localhost:6377/0' => %w(h q s y j m n o)}
    end
  end
end
