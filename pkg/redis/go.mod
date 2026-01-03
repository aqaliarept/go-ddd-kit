module github.com/aqaliarept/go-ddd-kit/pkg/redis

go 1.25.5

require (
	github.com/aqaliarept/go-ddd-kit/pkg/core v0.0.0
	github.com/redis/go-redis/v9 v9.17.0
)

replace github.com/aqaliarept/go-ddd-kit/pkg/core => ../core

replace github.com/aqaliarept/go-ddd-kit/internal/test => ../../internal/test

require (
	github.com/avast/retry-go/v4 v4.7.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/google/go-cmp v0.7.0 // indirect
)
