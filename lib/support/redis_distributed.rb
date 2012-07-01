class Redis
  class Distributed

    def initialize(urls, options = {})
      @tag = options.delete(:tag) || /^\{(.+?)\}/
      @default_options = options
      
      require "redis" unless options[:em]
      
      redises = urls.map do |url| 
        host, port = url_to_hostport(url) 
        if options[:em]
          EM::Hiredis::Client.connect(host, port, 1)
        else
          Redis.new(:host => host, :port => port, :db => 1)
        end
      end

      @ring = HashRing.new(redises)

      @subscribed_node = nil
    end

    def url_to_hostport(url)
      host, port = url.gsub(/redis:\/\//, '').split(/:/)
      [host, port.to_i]
    end
  end
end
