source 'http://rubygems.org'

gemspec

gem 'capistrano'
gem 'rvm1-capistrano3', require: false
# fixing sshkit version due to a capistrano-related bug
gem 'sshkit', '1.4.0'

# Specifying how-to-require
gem 'highline', require: 'highline/import'

unless RUBY_PLATFORM =~ /win32/
  if RUBY_VERSION < '2.0.0'
    gem 'pry'
    gem 'pry-debugger'
  else
    gem 'pry-byebug'
  end
end
