
NAME = sql-raft
REPO = github.com/euforia
VERSION = 0.1

deps:
	go get github.com/hashicorp/raft
	go get github.com/hashicorp/raft-boltdb

clean:
	rm -rf ./_tmp
	rm -f $(NAME)

build: clean
	go build -o $(NAME) *.go


docker-build:
	docker run --rm -v $(shell pwd):/go/src/$(REPO)/$(NAME) -w /go/src/$(REPO)/$(NAME) golang:1.6.2 make deps build

docker-image:
	docker build -t euforia/$(NAME):$(VERSION) .

docker: docker-build docker-image
