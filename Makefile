######################################################################
# @author      : Hung Nguyen Xuan Pham (hung0913208@gmail.com)
# @file        : Makefile
# @created     : Tuesday Jul 05, 2022 15:26:35 +07
######################################################################

env:
	go env -w GO111MODULE=on GOPROXY=direct GOFLAGS="-insecure"

tidy: env
	go mod tidy

test: tidy
	go test -v

build:

