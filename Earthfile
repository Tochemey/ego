VERSION 0.7
PROJECT tochemey/goakt


FROM tochemey/docker-go:1.20.1-0.7.0

protogen:
    # copy the proto files to generate
    COPY --dir protos/ ./
    COPY buf.work.yaml buf.gen.yaml ./

    # generate the pbs
    RUN buf generate \
            --template buf.gen.yaml \
            --path protos/ego

    # save artifact to
    SAVE ARTIFACT gen/ego/v1 AS LOCAL egopb

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
