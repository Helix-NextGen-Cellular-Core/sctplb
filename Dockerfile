# Copyright 2022-present Open Networking Foundation
#
# SPDX-License-Identifier: Apache-2.0
#

FROM golang:1.18.0-stretch AS lb
# FROM golang:1.18.3-stretch AS lb

LABEL maintainer="ONF <omec-dev@opennetworking.org>"

RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list
RUN apt-get update && apt-get -y install vim
RUN cd $GOPATH/src && mkdir -p sctplb
COPY . $GOPATH/src/sctplb
RUN cd $GOPATH/src/sctplb && make all 

# FROM lb AS sctplb
# WORKDIR /sdcore
# RUN mkdir -p /sdcore/bin
# COPY --from=lb $GOPATH/bin/* /sdcore/bin/
# WORKDIR /sdcore

FROM alpine:3.17 as sctplb
RUN apk update
RUN apk add -U vim strace net-tools curl netcat-openbsd bind-tools bash libc6-compat

WORKDIR /sdcore
RUN mkdir -p /sdcore/bin
COPY --from=lb /go/bin/* /sdcore/bin/
WORKDIR /sdcore
