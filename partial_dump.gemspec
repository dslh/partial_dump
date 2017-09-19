Gem::Specification.new do |s|
  s.name        = 'partial_dump'
  s.version     = '0.0.1'
  s.date        = '2017-09-18'
  s.summary     = 'PostgreSQL partial dump utility'
  s.description = 'Produces database dumps with finer-grained control than the standard psql utility'
  s.authors     = ['Doug Hammond']
  s.email       = 'douglas@gohiring.com'

  s.files = (Dir['lib/**/*'] + Dir['bin/*']).reject { |d| File.directory? d }

  s.bindir = 'bin'
  s.executables << 'partial_dump.rb'

  # Runtime dependencies
  %w(
    docile
    pg
    trollop
  ).each do |dep|
    s.add_runtime_dependency dep
  end

  # Development dependencies
  %w(
    rspec
    yard
    yard-struct
  ).each do |dep|
    s.add_development_dependency dep
  end
end
