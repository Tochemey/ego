VERSION 0.8

FROM tochemey/docker-go:1.21.0-1.0.0

RUN go install github.com/ory/go-acc@latest

protogen:
    # copy the proto files to generate
    COPY --dir protos/ ./
    COPY buf.work.yaml buf.gen.yaml ./

    # generate the pbs
    RUN buf generate \
            --template buf.gen.yaml \
            --path protos/ego

    # save artifact to
    SAVE ARTIFACT gen/ego/v3 AS LOCAL egopb

testprotogen:
    # copy the proto files to generate
    COPY --dir protos/ ./
    COPY buf.work.yaml buf.gen.yaml ./

    # generate the pbs
    RUN buf generate \
            --template buf.gen.yaml \
            --path protos/test/pb

    # save artifact to
    SAVE ARTIFACT gen/test AS LOCAL test/data

sample-pb:
    # copy the proto files to generate
    COPY --dir protos/ ./
    COPY buf.work.yaml buf.gen.yaml ./

    # generate the pbs
    RUN buf generate \
            --template buf.gen.yaml \
            --path protos/sample/pb

    # save artifact to
    SAVE ARTIFACT gen gen AS LOCAL example/pbs

pbs:
    BUILD +protogen
    BUILD +testprotogen
    BUILD +sample-pb

test:
  BUILD +lint
  BUILD +local-test

code:

    WORKDIR /app

    # download deps
    COPY go.mod go.sum ./
    RUN go mod download -x

    # copy in code
    COPY --dir . ./

vendor:
    FROM +code

	  COPY +mock/mocks ./mocks

    RUN go mod vendor
    SAVE ARTIFACT /app /files

lint:
    FROM +vendor

    COPY .golangci.yml ./
    # Runs golangci-lint with settings:
    RUN golangci-lint run --timeout 10m

local-test:
    FROM +vendor

    WITH DOCKER --pull postgres:11
        RUN go-acc ./... -o coverage.out --ignore egopb,test,example,mocks -- -mod=vendor -timeout 0 -race -v
    END

    SAVE ARTIFACT coverage.out AS LOCAL coverage.out

mock:
    # copy in the necessary files that need mock generated code
    FROM +code

    # generate the mocks
    RUN mockery  --dir eventstore --name EventsStore --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/eventstore --case snake
    RUN mockery  --dir offsetstore --name OffsetStore --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/offsetstore --case snake
    RUN mockery  --dir durablestore --name DurableStore --keeptree --exported=true --with-expecter=true --inpackage=true --disable-version-string=true --output ./mocks/durablestore --case snake

    SAVE ARTIFACT ./mocks mocks AS LOCAL mocks