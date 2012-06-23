$LOAD_PATH.unshift File.expand_path(File.join("..", File.dirname(__FILE__)))
$LOAD_PATH.unshift File.join(File.expand_path("..", File.dirname(`jruby -S gem which redis`)), "/test")

require "migrator.rb"
