#!/usr/bin/env bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex

export USER='logstash'

bundle exec rspec spec && bundle exec rspec spec --tag integration
