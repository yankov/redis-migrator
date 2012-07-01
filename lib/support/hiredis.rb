module EventMachine::Hiredis
  class Client
    def self.connect(host = 'localhost', port = 6379, db = nil)
      new(host, port, nil, db).connect
    end

    def id
      "redis://#{@host}:#{@port}/#{@db}"
    end
  end
end
