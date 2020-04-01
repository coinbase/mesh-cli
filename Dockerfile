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

FROM golang:1.13

WORKDIR $GOPATH/src/github.com/coinbase/rosetta-validator

# Copy Client
COPY go.sum ./go.sum
COPY go.mod ./go.mod
COPY main.go ./main.go
COPY internal/ ./internal

RUN GO111MODULE=on go install .

RUN mkdir /data
WORKDIR /app
ENV PATH /app:$PATH
RUN mv $GOPATH/bin/rosetta-validator .
