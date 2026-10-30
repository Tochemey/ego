module github.com/tochemey/ego/cmd/migrate

go 1.27.0

require (
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.12.1
	github.com/tochemey/ego/v5 v5.0.0
	github.com/tochemey/goakt/v4 v4.6.5
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sync v0.24.0 // indirect
)

replace github.com/tochemey/ego/v5 => ../../
