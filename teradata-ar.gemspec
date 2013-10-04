# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'teradata/ar/version'

Gem::Specification.new do |spec|
  spec.name          = "teradata-ar"
  spec.version       = Teradata::Ar::VERSION
  spec.authors       = ["Giuseppe Privitera"]
  spec.email         = ["priviterag@gmail.com"]
  spec.description   = %q{Teradata ActiveRecord adapter}
  spec.summary       = %q{Teradata ActiveRecord adapter}
  spec.homepage      = "https://github.com/priviterag/teradata-ar"
  spec.license       = "LGPL2"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  #spec.add_runtime_dependency "teradata-cli", "0.0.4"
end
