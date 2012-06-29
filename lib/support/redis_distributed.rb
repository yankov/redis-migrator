class Redis
  class Distributed

    def initialize(urls, options = {})
      @tag = options.delete(:tag) || /^\{(.+?)\}/
      @default_options = options
      @ring = HashRing.new urls.map { |url| host, port = url_to_hostport(url); EM::Hiredis::Client.connect(host, port, 1) }
      @subscribed_node = nil
    end

    def url_to_hostport(url)
      host, port = url.gsub(/redis:\/\//, '').split(/:/)
      [host, port.to_i]
    end

  end
end
