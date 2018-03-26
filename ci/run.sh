bundle install
bundle exec rake vendor
bundle exec rspec spec && bundle exec rspec spec --tag integration
