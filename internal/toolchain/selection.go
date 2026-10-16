package toolchain

// These standard packages replay initialization and route mutable globals
// to each instance. Other standard packages are still instrumented: their
// unlisted process state is denied to private execution.
// The JSON v2 initializer writes callback globals in internal and jsonopts;
// its readers need the same per-instance routing. Reflect is selected with
// JSON because its type caches can retain values created by an isolate. Time
// has lazy mutable zone state, but its clock and timer effects remain shared.
var isolateOwnedStandardPackages = map[string]bool{
	"encoding/binary":                  true,
	"math":                             true,
	"math/big":                         true,
	"math/rand":                        true,
	"math/rand/v2":                     true,
	"crypto/rand":                      true,
	"regexp":                           true,
	"regexp/syntax":                    true,
	"unicode":                          true,
	"unicode/utf8":                     true,
	"unicode/utf16":                    true,
	"io":                               true,
	"fmt":                              true,
	"log":                              true,
	"log/internal":                     true,
	"log/slog":                         true,
	"log/slog/internal":                true,
	"log/slog/internal/buffer":         true,
	"strconv":                          true,
	"bytes":                            true,
	"strings":                          true,
	"errors":                           true,
	"internal/reflectlite":             true,
	"context":                          true,
	"encoding/base32":                  true,
	"encoding/base64":                  true,
	"encoding/json":                    true,
	"encoding/json/internal":           true,
	"encoding/json/internal/jsonwire":  true,
	"encoding/json/internal/jsonflags": true,
	"encoding/json/internal/jsonopts":  true,
	"encoding/json/jsontext":           true,
	"encoding/json/v2":                 true,
	"reflect":                          true,
	"time":                             true,
}

// Value codecs and pools belong to the caller. Descriptor builders and default
// registries remain process services with pinned, narrow metadata privileges.
func isolateOwnedProtobufPackage(path string) bool {
	switch path {
	case "google.golang.org/protobuf/proto", "google.golang.org/protobuf/encoding/protojson", "google.golang.org/protobuf/encoding/prototext",
		"google.golang.org/protobuf/reflect/protoreflect",
		"google.golang.org/protobuf/internal/impl", "google.golang.org/protobuf/internal/order",
		"google.golang.org/protobuf/internal/detrand", "google.golang.org/protobuf/internal/errors",
		"google.golang.org/protobuf/internal/encoding/json", "google.golang.org/protobuf/internal/encoding/text":
		return true
	}
	return false
}

// OTel API attribute encoders and header parsing caches are private, including
// scratch copies for read-only handlers. The trace API initializes an automatic
// tracer from process environment; keep that startup on the host. Native sink
// providers use only its value/context API, never the automatic/global provider.
func isolateOwnedObservabilityPackage(path string) bool {
	switch path {
	// The active read-only interceptor chain must be scratch-owned even when
	// a query closure passes a context captured from the writable workflow.
	case "github.com/mfateev/sdk-go-poc/internal/interceptorscope":
		return true
	// OpenTracing's context key, error values, optional global tracer and ext
	// tag descriptors are ordinary private package state. Replay their startup
	// in query/validator scratch layouts too; grant no process-state access.
	case "github.com/opentracing/opentracing-go", "github.com/opentracing/opentracing-go/log", "github.com/opentracing/opentracing-go/ext":
		return true
	case "go.opentelemetry.io/otel/attribute", "go.opentelemetry.io/otel/attribute/internal", "go.opentelemetry.io/otel/baggage", "go.opentelemetry.io/otel/internal/baggage", "go.opentelemetry.io/otel/propagation":
		return true
	}
	return false
}
