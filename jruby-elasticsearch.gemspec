$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "jruby-elasticsearch/version"

Gem::Specification.new do |spec|
  files = []
  dirs = %w{lib examples etc patterns test}
  dirs.each do |dir|
    files += Dir["#{dir}/**/*"]
  end

  spec.name = "telmate-jruby-elasticsearch"
  spec.version = ElasticSearch::VERSION
  spec.summary = "JRuby API for ElasticSearch using the native ES Java API"
  spec.description = "..."
  spec.license = "Apache License (2.0)"

  spec.files = files
  spec.require_paths << "lib"
  #spec.bindir = "bin"
  #spec.executables << "..."

  spec.authors = ["Jordan Sissel"]
  spec.email = ["jls@semicomplete.com"]
  spec.homepage = "https://github.com/jordansissel/jruby-elasticsearch"
end

