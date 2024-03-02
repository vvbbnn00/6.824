# Description: Build wc plugin

# Build wc plugin
go build -buildmode=plugin ../mrapps/wc.go
# Clean up
rm -f mr-*