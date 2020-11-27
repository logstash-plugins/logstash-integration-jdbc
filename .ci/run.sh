#!/usr/bin/env bash

# This is intended to be run inside the docker container as the command of the docker-compose.
set -ex

export USER='logstash'

export LOG_LEVEL='trace'

jruby -rbundler/setup -S rspec -fd && jruby -rbundler/setup -S rspec -fd --tag integration
