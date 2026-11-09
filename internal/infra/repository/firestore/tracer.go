package firestore

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("repository")
