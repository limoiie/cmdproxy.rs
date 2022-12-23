#!/usr/bin/env bash

VERSION=`cat Cargo.toml | grep '^version' | awk -F'=' '{print $2}' | tr -d ' "'`
export CMDPROXY_VERSION=$VERSION
echo $CMDPROXY_VERSION
