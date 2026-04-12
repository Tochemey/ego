module github.com/tochemey/ego/v4

go 1.26.0

require (
	github.com/deckarep/golang-set/v2 v2.8.0
	github.com/flowchartsman/retry v1.2.0
	github.com/google/uuid v1.6.0
	github.com/kapetan-io/tackle v0.13.0
	github.com/stretchr/testify v1.11.1
	github.com/tochemey/goakt/v4 v4.2.0
	github.com/travisjeffery/go-dynaport v1.0.0
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/metric v1.43.0
	go.opentelemetry.io/otel/sdk v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	golang.org/x/sync v0.20.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/Workiva/go-datastructures v1.1.7 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/hashicorp/memberlist v0.5.4 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/miekg/dns v1.1.72 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/redis/go-redis/v9 v9.18.0 // indirect
	github.com/reugn/go-quartz v0.15.2 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/redcon v1.6.2 // indirect
	github.com/tochemey/olric v0.3.9 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/tools v0.44.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// HashiCorp renamed github.com/armon/go-metrics to github.com/hashicorp/go-metrics
// in v0.4.2 and every release since declares the new module path, so they fail
// to satisfy the legacy import path that hashicorp/go-metrics/compat still
// pulls in transitively (via memberlist → goakt → ego). v0.4.1 is the last
// version that resolves under the armon path; exclude the broken ones so
// `go mod tidy` and `go get -u` stop probing them.
exclude (
	github.com/armon/go-metrics v0.4.2
	github.com/armon/go-metrics v0.5.0
	github.com/armon/go-metrics v0.5.1
	github.com/armon/go-metrics v0.5.2
	github.com/armon/go-metrics v0.5.3
	github.com/armon/go-metrics v0.5.4
)
