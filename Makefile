GO_VERSION := 1.13


image: dev
	docker build -t jcodybaker/flipop .

dev:
	docker build --no-cache -t flipop-dev -f Dockerfile.dev \
		--build-arg=GO_IMAGE=docker.io/golang:$(GO_VERSION)-buster .

generate-k8s:
	docker run -v $$(pwd):/go/src/github.com/jcodybaker/flipop/ \
		flipop-dev:latest \
		/go/src/k8s.io/code-generator/generate-groups.sh \
		all \
		github.com/jcodybaker/flipop/pkg/apis/flipop/generated \
		github.com/jcodybaker/flipop/pkg/apis \
		flipop:v1alpha1 \
		--go-header-file=hack/boilerplate.go.txt

image-push: 
	docker push jcodybaker/flipop

.PHONY: dev image
