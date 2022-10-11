#!/usr/bin/env bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex

export USER='logstash'

bundle exec rspec spec --format documentation && bundle exec rspec spec --format documentation --tag integration
