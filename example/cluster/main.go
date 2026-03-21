// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package main demonstrates a production-ready eGo cluster running on
// Kubernetes with:
//
//   - Kubernetes-native peer discovery (pods find each other via the k8s API)
//   - PostgreSQL-backed event store and offset store
//   - A projection that materializes account balances into a read table (CQRS)
//   - OpenTelemetry traces and metrics (exported via OTLP to Jaeger and Prometheus)
//   - A simple HTTP API for sending commands and querying the read side
//
// See the Makefile for how to build, deploy, and test this example on a Kind cluster.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	gerrors "github.com/tochemey/goakt/v4/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"github.com/tochemey/ego/v4"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/projection"
)

const projectionName = "account-balances"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := requireEnv("POSTGRES_DSN")
	nodeIP := requireEnv("NODE_IP")
	namespace := requireEnv("NAMESPACE")
	remotingPort := envInt("REMOTING_PORT", 9000)
	discoveryPort := envInt("DISCOVERY_PORT", 9001)
	peersPort := envInt("PEERS_PORT", 9002)
	httpPort := envInt("HTTP_PORT", 8080)

	tel, telShutdown, err := setupTelemetry(ctx, "ego-cluster")
	if err != nil {
		slog.Error("failed to setup telemetry", "err", err)
		os.Exit(1)
	}
	defer telShutdown(context.Background())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("failed to create connection pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	eventStore := NewPostgresEventStore(dsn)
	if err := eventStore.Connect(ctx); err != nil {
		slog.Error("failed to connect event store", "err", err)
		os.Exit(1)
	}
	defer eventStore.Disconnect(ctx)

	offsetStore := NewPostgresOffsetStore(dsn)
	if err := offsetStore.Connect(ctx); err != nil {
		slog.Error("failed to connect offset store", "err", err)
		os.Exit(1)
	}
	defer offsetStore.Disconnect(ctx)

	provider := NewKubernetesProvider(
		namespace,
		map[string]string{"app": "ego-cluster"},
		"discovery",
		"remoting",
		"peers",
	)

	projectionHandler := NewAccountBalanceHandler(pool)

	engine := ego.NewEngine("ego-cluster", eventStore,
		ego.WithCluster(provider, 4, 1, nodeIP, remotingPort, discoveryPort, peersPort),
		ego.WithOffsetStore(offsetStore),
		ego.WithTelemetry(tel),
		ego.WithProjection(&projection.Options{
			Handler:      projectionHandler,
			BufferSize:   500,
			StartOffset:  time.Time{},
			ResetOffset:  time.Time{},
			PullInterval: time.Second,
			Recovery: projection.NewRecovery(
				projection.WithRetries(5),
				projection.WithRetryDelay(time.Second),
				projection.WithRecoveryPolicy(projection.RetryAndSkip),
			),
		}),
	)

	if err := engine.Start(ctx); err != nil {
		slog.Error("failed to start engine", "err", err)
		os.Exit(1)
	}

	// Start the projection runner.
	// In cluster mode, AddProjection automatically runs it as a singleton on the
	// oldest node. If that node leaves, it migrates to the new oldest node.
	if err := engine.AddProjection(ctx, projectionName); err != nil {
		slog.Error("failed to add projection", "err", err)
		os.Exit(1)
	}

	slog.Info("engine started",
		"node", nodeIP,
		"remotingPort", remotingPort,
		"discoveryPort", discoveryPort,
		"peersPort", peersPort,
	)

	mux := http.NewServeMux()

	// Health/readiness probe.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	// Create an account: POST /accounts/{id} {"balance": 1000}
	mux.HandleFunc("POST /accounts/{id}", func(w http.ResponseWriter, r *http.Request) {
		accountID := r.PathValue("id")
		var body struct {
			Balance float64 `json:"balance"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		behavior := NewAccountBehavior(accountID)
		if err := entityWithRetry(r.Context(), engine, behavior); err != nil {
			http.Error(w, fmt.Sprintf("entity error: %v", err), http.StatusInternalServerError)
			return
		}

		cmd := &samplepb.CreateAccount{
			AccountId:      accountID,
			AccountBalance: body.Balance,
		}
		reply, _, err := sendCommandWithRetry(r.Context(), engine, accountID, cmd, 10*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusCreated, reply)
	})

	// Credit an account: POST /accounts/{id}/credit {"amount": 250}
	mux.HandleFunc("POST /accounts/{id}/credit", func(w http.ResponseWriter, r *http.Request) {
		accountID := r.PathValue("id")
		var body struct {
			Amount float64 `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reply, _, err := sendCommandWithRetry(r.Context(), engine, accountID, &samplepb.CreditAccount{
			AccountId: accountID,
			Balance:   body.Amount,
		}, 10*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusOK, reply)
	})

	// Debit an account: POST /accounts/{id}/debit {"amount": 100}
	mux.HandleFunc("POST /accounts/{id}/debit", func(w http.ResponseWriter, r *http.Request) {
		accountID := r.PathValue("id")
		var body struct {
			Amount float64 `json:"amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reply, _, err := sendCommandWithRetry(r.Context(), engine, accountID, &samplepb.DebitAccount{
			AccountId: accountID,
			Balance:   body.Amount,
		}, 10*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusOK, reply)
	})

	// Query account balance from the projection (read side):
	// GET /accounts/{id}
	mux.HandleFunc("GET /accounts/{id}", func(w http.ResponseWriter, r *http.Request) {
		accountID := r.PathValue("id")

		var balance float64
		var updatedAt int64
		err := pool.QueryRow(r.Context(),
			`SELECT balance, updated_at FROM account_balances WHERE account_id = $1`,
			accountID,
		).Scan(&balance, &updatedAt)
		if err != nil {
			http.Error(w, fmt.Sprintf("account not found: %v", err), http.StatusNotFound)
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"account_id": accountID,
			"balance":    balance,
			"updated_at": updatedAt,
			"source":     "projection",
		})
	})

	// Middleware chain (outermost → innermost):
	//   servedByMiddleware → otelhttp → mux
	//
	// otelhttp creates a server span per request, injects the span context
	// into r.Context(), and extracts incoming W3C traceparent headers so
	// that cross-service traces are stitched together automatically.
	//
	// The full trace in Jaeger looks like:
	//   HTTP POST /accounts/{id}   ← otelhttp server span
	//     └─ ego.send_command      ← engine.SendCommand
	//          └─ ego.command      ← EventSourcedActor
	//
	// X-Served-By lets callers see which pod handled the request.
	hostname, _ := os.Hostname()
	servedByMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Served-By", hostname)
			next.ServeHTTP(w, r)
		})
	}

	// Explicitly wire the tracer provider and propagator so otelhttp never
	// falls back to a noop global (guards against subtle init-order races).
	// spanNameFromRequest gives each route a clean name in Jaeger, e.g.
	// "POST /accounts/{id}" instead of the generic "ego-cluster-http".
	handler := servedByMiddleware(otelhttp.NewHandler(mux, "ego-cluster-http",
		otelhttp.WithTracerProvider(otel.GetTracerProvider()),
		otelhttp.WithPropagators(otel.GetTextMapPropagator()),
		otelhttp.WithSpanNameFormatter(spanNameFromRequest),
	))

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", httpPort),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start HTTP server in background.
	go func() {
		slog.Info("http server listening", "port", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	_ = server.Shutdown(shutdownCtx)
	_ = engine.Stop(shutdownCtx)
	slog.Info("shutdown complete")
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

func envInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Error("invalid integer env var", "key", key, "value", v)
		os.Exit(1)
	}
	return n
}

// entityWithRetry calls engine.Entity and retries up to 5 times on transient
// cluster errors (e.g. olric state query timeouts under concurrent load on the
// projection-leader pod). ErrActorAlreadyExists is always treated as success.
func entityWithRetry(ctx context.Context, engine *ego.Engine, behavior ego.EventSourcedBehavior) error {
	const maxAttempts = 5
	var lastErr error
	for attempt := range maxAttempts {
		err := engine.Entity(ctx, behavior)
		if err == nil || errors.Is(err, gerrors.ErrActorAlreadyExists) {
			return nil
		}
		lastErr = err
		if attempt < maxAttempts-1 {
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
	}
	return lastErr
}

// sendCommandWithRetry sends a command and retries on ErrActorNotFound.
// After SpawnOn places an entity on a remote peer, the cluster's distributed
// state (olric) may not have propagated the actor's location yet. A short
// retry loop lets the state converge before giving up.
func sendCommandWithRetry(ctx context.Context, engine *ego.Engine, entityID string, cmd ego.Command, timeout time.Duration) (ego.State, uint64, error) {
	const maxAttempts = 5
	for attempt := range maxAttempts {
		state, revision, err := engine.SendCommand(ctx, entityID, cmd, timeout)
		if err == nil {
			return state, revision, nil
		}
		if !errors.Is(err, gerrors.ErrActorNotFound) {
			return nil, 0, err
		}
		if attempt < maxAttempts-1 {
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
	}
	return engine.SendCommand(ctx, entityID, cmd, timeout)
}

// spanNameFromRequest returns a clean span name for HTTP requests.
// Uses r.Pattern when set (Go 1.22+ method-specific routes like "POST /accounts/{id}"
// already include the method, so we avoid duplication). Otherwise "METHOD /path".
func spanNameFromRequest(_ string, r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return r.Method + " " + r.URL.Path
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
