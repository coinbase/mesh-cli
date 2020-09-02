#!/bin/sh

VERSION=$1;

xgo --targets=darwin/*,windows/*,linux/* -out "bin/rosetta-cli-${VERSION}" .;

# Rename some files
mv "bin/rosetta-cli-${VERSION}-darwin-10.6-amd64" "bin/rosetta-cli-${VERSION}-darwin-amd64" 
mv "bin/rosetta-cli-${VERSION}-windows-4.0-amd64.exe" "bin/rosetta-cli-${VERSION}-windows-amd64"

# Tar all files
cd bin || exit;
for i in *; do tar -czf "$i.tar.gz" "$i" && rm "$i"; done
