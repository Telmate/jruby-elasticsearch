# Require all the elasticsearch libs
["#{ENV['ELASTICSEARCH_HOME']}/**/*.jar"].each do |path|
  Dir.glob(path).each do |jar|
    require jar
  end
end

$:.unshift("lib")

require "rubygems"
require "jruby-elasticsearch"
