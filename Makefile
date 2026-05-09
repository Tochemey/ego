.PHONY: run-eventsourced run-durablestate run-saga proto \
        docker-image docker-lint docker-test docker-mock docker-protogen docker-ci

# ---------------------------------------------------------------------------
# Local developer targets
# ---------------------------------------------------------------------------

# Run the event-sourced example
run-eventsourced:
	@echo "Running event-sourced example..."
	go run ./example/eventssourced

# Run the durable state example
run-durablestate:
	@echo "Running durable state example..."
	go run ./example/durablestate

# Run the fund transfer saga example
run-saga:
	@echo "Running fund transfer saga example..."
	go run ./example/saga

# Regenerate protobuf code (uses the locally installed `buf`).
# buf writes everything under gen/ (per buf.gen.yaml). We then copy each
# subtree to its destination — matching the layout the previous Earthfile
# produced via SAVE ARTIFACT.
proto:
	@echo "Generating protobuf code..."
	buf generate
	cp -R gen/ego/.    egopb/
	cp -R gen/test/.   test/data/testpb/
	cp -R gen/sample/. example/examplepb/
	rm -rf gen
	@echo "Done."

# ---------------------------------------------------------------------------
# Docker-based CI targets
#
# These targets replicate what the Earthfile used to do: build a hermetic
# tooling image (Dockerfile.ci) and run lint, tests, mock generation, and
# protobuf generation inside a container so contributors and CI do not need
# the toolchain installed locally.
# ---------------------------------------------------------------------------

DOCKER_IMAGE       ?= ego-ci:latest
DOCKER_DOCKERFILE  ?= Dockerfile.ci
WORKDIR_IN_CONT    ?= /workspace

# Cache volumes keep the Go build/module caches warm between runs so repeated
# invocations are fast. They are mounted onto image-prepared paths so they
# inherit world-writable permissions and can be used by any host UID.
# Versioned suffix ('-v2') so older volumes from earlier (broken) versions
# of this Makefile are not reused.
GO_BUILD_CACHE_VOL ?= ego-go-build-cache-v2
GO_MOD_CACHE_VOL   ?= ego-go-mod-cache-v2

# Run the tooling image with the working tree mounted at $(WORKDIR_IN_CONT)
# and shared Go caches under /cache (prepared 0777 in Dockerfile.ci).
# `--user` keeps generated files owned by the current user on Linux hosts
# (a no-op on Docker Desktop for macOS). HOME points at /home/build so any
# tool that defaults to $HOME-based caches (e.g. golangci-lint) has a
# writable location too.
DOCKER_RUN = docker run --rm -t \
	--user $$(id -u):$$(id -g) \
	-e HOME=/home/build \
	-e GOCACHE=/cache/go-build \
	-e GOMODCACHE=/cache/go-mod \
	-v $(CURDIR):$(WORKDIR_IN_CONT) \
	-v $(GO_BUILD_CACHE_VOL):/cache/go-build \
	-v $(GO_MOD_CACHE_VOL):/cache/go-mod \
	-w $(WORKDIR_IN_CONT) \
	$(DOCKER_IMAGE)

# Build the CI tooling image. Re-run after changing Dockerfile.ci.
docker-image:
	@echo "Building CI image $(DOCKER_IMAGE)..."
	docker build -f $(DOCKER_DOCKERFILE) -t $(DOCKER_IMAGE) .

# Run golangci-lint inside the CI image.
docker-lint: docker-image
	@echo "Running golangci-lint..."
	$(DOCKER_RUN) golangci-lint run --timeout 10m

# Run the test suite with race detection and coverage inside the CI image.
# The PKGS list mirrors the Earthfile's `local-test` target: it excludes
# generated, example, and mock packages from coverage reporting.
docker-test: docker-image
	@echo "Running tests with race detector..."
	$(DOCKER_RUN) sh -c '\
		PKGS=$$(go list -mod=vendor ./... | grep -v -E "(egopb|test|example|mocks)") && \
		go test -mod=vendor -p 1 -timeout 0 -race -v \
			-coverprofile=coverage.out -covermode=atomic \
			-coverpkg=$$(echo $$PKGS | tr " " ",") \
			$$PKGS'

# Regenerate mocks via mockery inside the CI image. Output is written to ./mocks.
docker-mock: docker-image
	@echo "Generating mocks..."
	$(DOCKER_RUN) sh -c '\
		mockery --dir persistence  --all                  --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/persistence  --case snake && \
		mockery --dir offsetstore  --name OffsetStore     --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/offsetstore  --case snake && \
		mockery --dir .            --name EventPublisher  --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/ego          --case snake && \
		mockery --dir .            --name StatePublisher  --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/ego          --case snake && \
		mockery --dir encryption   --all                  --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/encryption   --case snake && \
		mockery --dir eventadapter --all                  --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/eventadapter --case snake'

# Regenerate protobuf code inside the CI image. buf writes everything under
# gen/ (per buf.gen.yaml); we then copy each subtree to its destination —
# matching the layout the previous Earthfile produced via SAVE ARTIFACT.
docker-protogen: docker-image
	@echo "Generating protobuf code..."
	$(DOCKER_RUN) sh -c '\
		buf generate \
			--template buf.gen.yaml \
			--path protos/ego \
			--path protos/test \
			--path protos/sample && \
		cp -R gen/ego/.    egopb/ && \
		cp -R gen/test/.   test/data/testpb/ && \
		cp -R gen/sample/. example/examplepb/ && \
		rm -rf gen'

# Composite target: lint + test, the same combination the Earthfile `test`
# target used to BUILD.
docker-ci: docker-lint docker-test
