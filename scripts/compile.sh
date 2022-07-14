#!/bin/sh
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


VERSION=$1;

go install github.com/crazy-max/xgo@latest

MAC_TARGETS="darwin/amd64,darwin/arm64"
LINUX_TARGETS="linux/amd64,linux/arm64,linux/mips64,linux/mips64le,linux/ppc64le,linux/s390x"
WINDOWS_TARGET="windows/amd64"
TARGETS="${MAC_TARGETS},${LINUX_TARGETS},${WINDOWS_TARGET}"

xgo -go 1.16.3 --targets=${TARGETS} -out "bin/rosetta-cli-${VERSION}" .;

# Rename some files
mv "bin/rosetta-cli-${VERSION}-darwin-10.16-amd64" "bin/rosetta-cli-${VERSION}-darwin-amd64"
mv "bin/rosetta-cli-${VERSION}-darwin-10.16-arm64" "bin/rosetta-cli-${VERSION}-darwin-arm64"
mv "bin/rosetta-cli-${VERSION}-windows-4.0-amd64.exe" "bin/rosetta-cli-${VERSION}-windows-amd64"

# Tar all files
cd bin || exit;
for i in *; do tar -czf "$i.tar.gz" "$i" && rm "$i"; done

go mod tidy
