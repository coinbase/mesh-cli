// Copyright 2022 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-sdk-go/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func InitProvider(config *configuration.Configuration) (func(context.Context) error, error) {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceName("checkdata-service"),
		))
	if err != nil {
		return nil, fmt.Errorf("tracing error: failed to create resource: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, config.OtelCollectorURL,
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing error: failed to create connection with exporter: %w", err)
	}

	// setup trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("tracing error: failed to create trace exporter: %w", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// set as global trace provider
	otel.SetTracerProvider(traceProvider)
	log.Printf("tracing initialized")
	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return traceExporter.Shutdown, nil
}

func NewTracedClient(config *configuration.Configuration) *client.APIClient {

	defaultTransport := http.DefaultTransport.(*http.Transport).Clone()
	defaultTransport.IdleConnTimeout = 30 * time.Second
	defaultTransport.MaxIdleConns = config.MaxOnlineConnections
	defaultTransport.MaxIdleConnsPerHost = 120
	tracedHTTPClient := &http.Client{
		Timeout:   time.Duration(config.HTTPTimeout) * time.Second,
		Transport: otelhttp.NewTransport(defaultTransport, otelhttp.WithSpanNameFormatter(transportFormatter)),
	}

	// Create traced fetcher
	clientCfg := client.NewConfiguration(
		config.OnlineURL,
		"rosetta-sdk-go",
		tracedHTTPClient,
	)

	tracedClient := client.NewAPIClient(clientCfg)
	return tracedClient
}

func transportFormatter(_ string, r *http.Request) string {
	return r.URL.Path
}
