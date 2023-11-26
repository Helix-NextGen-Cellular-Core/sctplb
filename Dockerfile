# Copyright 2022-present Open Networking Foundation
#
# SPDX-License-Identifier: Apache-2.0
#

FROM golang:1.18.3-stretch AS builder

LABEL maintainer="ONF <omec-dev@opennetworking.org>"

RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list
RUN apt-get update
RUN apt-get -y install apt-transport-https ca-certificates
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg > pubkey.gpg
RUN apt-key add pubkey.gpg
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" |  tee /etc/apt/sources.list.d/yarn.list
RUN apt-get update
RUN apt-get -y install gcc cmake autoconf libtool pkg-config libmnl-dev libyaml-dev  nodejs yarn
RUN apt-get clean

RUN cd $GOPATH/src && mkdir -p sctplb
COPY . $GOPATH/src/sctplb
RUN cd $GOPATH/src/sctplb && make all 

FROM alpine:3.17 as sctplb
RUN apk update
RUN apk add -U vim strace net-tools curl netcat-openbsd bind-tools bash libc6-compat
WORKDIR /sdcore
RUN mkdir -p /sdcore/bin
COPY --from=builder /go/src/sctplb/bin/* /sdcore/bin/
WORKDIR /sdcore
