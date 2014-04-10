shared_examples 'pretested migrator' do
  let(:keys) { %w(q s j) }
  let(:node) { { host: 'localhost', port: 6378, db: 1 } }
  let(:options) { {} }

  before do
    prefill_cluster(old_cluster)
    migrator.migrate(node, keys, options)
  end

  def common_keys(cluster)
    (cluster.keys('*') & keys).sort
  end

  subject { common_keys(cluster) }

  context do
    let(:cluster) { new_cluster }
    it 'should copy given keys to a new cluster' do
      should == %w(j q s)
    end
  end

  context do
    let(:cluster) { old_cluster }

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
