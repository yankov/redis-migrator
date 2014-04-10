class Redis
  module Helper
    def to_redis_proto(*cmd)
      cmd.inject("*#{cmd.length}\r\n") {|acc, arg|
        acc << "$#{arg.to_s.bytesize}\r\n#{arg}\r\n"
      }
    end

    def parse_redis_url(redis_url)
      node = URI(redis_url)
      path = node.path
      db = path[1..-1].to_i rescue 0

      {
        host: node.host,
        port: node.port || 6379,
        db:   db
      }
    end
  end
end
