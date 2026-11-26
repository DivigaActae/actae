module github.com/divigaactae/actae/examples/coordination

go 1.25.13

require github.com/divigaactae/actae/sdks/go v0.0.0

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
)

replace github.com/divigaactae/actae/sdks/go => ../../sdks/go
