## Key Sign Tool

Rosetta CLI has a key sign tool, which you can use to sign and verify various curves supported
by rosetta-specifications. This should only be used for local development. Never share private keys anywhere.

### Usage
#### Key Generate
```
rosetta-cli key:gen --curve-type secp256k1
```
Curve Type options are specified by [rosetta-specifications](https://github.com/coinbase/rosetta-specifications/blob/master/models/CurveType.yaml)
#### Sign
```
rosetta-cli key:sign --configuration-file config.json
```

A sample config file is located [here](../examples/configuration/sign.json)

Required fields includes
- `pub_key`
- `private_key`
- `signing_payload`


#### Verify
```
rosetta-cli key:verify --configuration-file verify.json
```
A sample config file is located [here](../examples/configuration/verify.json)

Required fields includes
- `pub_key`
- `signing_payload`
- `signature`

### Troubleshoot
- `account_identifier` field in `signing_payload` field should've a dummy address for providing valid payload.