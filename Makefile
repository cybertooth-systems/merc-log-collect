build-docker:
	docker build -t merc-log-collect:latest --target build .

build-linux-amd64:
	mkdir -p $(realpath ./bin/linux-amd64/) && \
		DOCKER_BUILDKIT=1 docker build --target bin --platform linux/amd64 \
		--output type=local,dest=$(realpath ./bin/linux-amd64/) .

datagen:
	go run ./cmd/datagen/.
