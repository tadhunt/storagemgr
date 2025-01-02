all:
	go mod tidy
	go vet
	staticcheck
	go build
