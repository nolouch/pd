#!/usr/bin/env bash
git submodule update --remote
cd pkg/ui/pd-web
yarn
PUBLIC_URL=/ui/ yarn build
go-bindata -pkg dist -prefix=build build/...
echo 'func init() { ui.Asset = Asset; ui.AssetDir = AssetDir; ui.AssetInfo = AssetInfo }' >> bindata.go
gofmt -s -w bindata.go
goimports -w bindata.go
mv bindata.go ../dist


