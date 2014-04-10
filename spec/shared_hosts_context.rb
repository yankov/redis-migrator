shared_context 'shared hosts context' do
  let(:old_hosts) { %w(redis://localhost:6379 redis://localhost:6378) }
  let(:new_hosts) { old_hosts + ['redis://localhost:6377'] }

  before do
    expect(Redis).to receive(:new).at_least(1).times do |options|
      MockRedis.new(options)
    end
  end
end
