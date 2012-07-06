# -*- encoding: utf-8 -*-
$:.push File.expand_path("./lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "redis_migrator"
  s.version     = "0.0.1"
  s.date        = "2012-07-05"
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Artem Yankov"]
  s.email       = ["artem.yankov@gmail.com"]
  s.homepage    = "http://rubygems.org/gems/redis_migrator"
  s.summary     = %q{A tool to redistribute keys in your redis cluster when its topography has changed}
  s.description = %q{Redis-migrator takes a list of nodes for your old cluster and list of nodes for your new cluster and determines for which keys routes were changed. Then it moves those keys to new nodes.}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end