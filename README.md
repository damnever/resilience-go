## resilience-go

[![Go Reference](https://pkg.go.dev/badge/github.com/damnever/resilience-go.svg)](https://pkg.go.dev/github.com/damnever/resilience-go)

A few of useful resilience patterns for Golang service.

The circuitbreaker package is a logical copy from [Netflix Hystrix](https://github.com/Netflix/Hystrix/wiki/How-it-Works#CircuitBreaker).
The concurrency package is modified from [Envoy Proxy](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter) and [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits).

`concurrency.New(limit.NewGradientLimit(...))` is the first choice of server-side protection.
