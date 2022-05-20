# Copyright 2020 Coinbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Compile golang
FROM ubuntu:20.04 as cli

RUN apt-get update && apt-get install -y curl make gcc g++ git
ENV GOLANG_VERSION 1.17.9
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 9dacf782028fdfc79120576c872dee488b81257b1c48e9032d122cfdb379cca6

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
  && echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
  && tar -C /usr/local -xzf golang.tar.gz \
  && rm golang.tar.gz

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

WORKDIR /go/src

ARG VERSION=v0.7.9
RUN git clone https://github.com/coinbase/rosetta-cli.git && \
	cd rosetta-cli && \
	git fetch --all --tags && \
	git checkout $VERSION && \
	make install

FROM ubuntu:20.04

RUN apt-get update -y && apt-get install -y \
	curl

# Copy all the binaries
COPY --from=cli /go/bin/ /usr/local/bin/

WORKDIR /app
ENTRYPOINT ["rosetta-cli"]