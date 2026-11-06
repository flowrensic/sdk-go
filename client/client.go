// Package client connects a Go worker to the Flowrensic server, beside
// go.temporal.io/sdk/client.
package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/sdk/internal/gen/flowrensic/v1/flowrensicv1connect"
)

// CurrentEnvHeader is the header every Flowrensic call carries the env in.
const CurrentEnvHeader = "Flowrensic-Current-Env"

// EnvAccessKeyHeader is the header every Flowrensic call carries the env access key in.
const EnvAccessKeyHeader = "Flowrensic-Env-Access-Key"

// CurrentEphemeralRunHeader is the header every Flowrensic call carries the ephemeral run in.
const CurrentEphemeralRunHeader = "Flowrensic-Current-Ephemeral-Run"

// DefaultTimeoutMs is how long the hello may take, in milliseconds, when
// Options carries no TimeoutMs.
const DefaultTimeoutMs = 5000

// Options is what a worker tells Flowrensic about itself.
type Options struct {
	// ServerBaseURL is the Flowrensic server, such as http://flowrensic:8080.
	ServerBaseURL string
	// CurrentEnv is the env this worker runs in, as the Flowrensic env list
	// names it, sent in the Flowrensic-Current-Env header of every call.
	CurrentEnv string
	// EnvAccessKey is the access key of that env, sent in the
	// Flowrensic-Env-Access-Key header of every call.
	EnvAccessKey string
	// CurrentEphemeralRun is which run of that env this is, sent in the
	// Flowrensic-Current-Ephemeral-Run header of every call. Empty, it has none.
	CurrentEphemeralRun string
	// WorkerID names this worker among the workers of the ephemeral run.
	// Empty, the host name and the process id joined by a dash.
	WorkerID string
	// Version is the version this worker serves. Empty, none.
	Version string
	// TimeoutMs is how long the hello may take, in milliseconds. Zero,
	// DefaultTimeoutMs.
	TimeoutMs int
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// Answer is what Flowrensic answered the hello with.
type Answer struct {
	// Forward is whether the worker is asked to send the engine's metrics.
	Forward bool
	// IntervalSeconds is how often it would send them.
	IntervalSeconds int32
	// SkipHistories is whether Flowrensic pulls the histories itself.
	SkipHistories bool
}

// Client is a worker's connection to the Flowrensic server.
type Client struct {
	options Options
	ingest  flowrensicv1connect.IngestServiceClient
	answer  Answer
}

// Connect says hello to the Flowrensic server, so the ephemeral run and the
// worker show up there, and returns the client. A refused hello or one with
// no answer returns the error, whose code connect.CodeOf reads.
func Connect(ctx context.Context, options Options) (*Client, error) {
	if options.ServerBaseURL == "" {
		return nil, errors.New("flowrensic: ServerBaseURL is required")
	}
	if options.WorkerID == "" {
		options.WorkerID = defaultWorkerID()
	}
	if options.TimeoutMs <= 0 {
		options.TimeoutMs = DefaultTimeoutMs
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	c := &Client{
		options: options,
		ingest: flowrensicv1connect.NewIngestServiceClient(
			httpClient,
			strings.TrimRight(options.ServerBaseURL, "/"),
			connect.WithInterceptors(contextInterceptor(options)),
		),
	}
	if err := c.hello(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// WorkerID is the worker id the hello carried.
func (c *Client) WorkerID() string { return c.options.WorkerID }

// Answer is what Flowrensic answered the hello with.
func (c *Client) Answer() Answer { return c.answer }

func (c *Client) hello(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.options.TimeoutMs)*time.Millisecond)
	defer cancel()
	res, err := c.ingest.EphemeralRunHandshake(ctx, connect.NewRequest(&flowrensicv1.EphemeralRunHandshakeRequest{
		WorkerId: c.options.WorkerID,
		Version:  c.options.Version,
	}))
	if err != nil {
		return fmt.Errorf("flowrensic: hello: %w", err)
	}
	c.answer = Answer{
		Forward:         res.Msg.GetForward(),
		IntervalSeconds: res.Msg.GetIntervalSeconds(),
		SkipHistories:   res.Msg.GetSkipHistories(),
	}
	return nil
}

// contextInterceptor puts the env, its access key and the ephemeral run of
// options in the headers of every call. An empty value sends no header.
func contextInterceptor(options Options) connect.UnaryInterceptorFunc {
	headers := [][2]string{
		{CurrentEnvHeader, options.CurrentEnv},
		{EnvAccessKeyHeader, options.EnvAccessKey},
		{CurrentEphemeralRunHeader, options.CurrentEphemeralRun},
	}
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			for _, h := range headers {
				if h[1] != "" {
					req.Header().Set(h[0], h[1])
				}
			}
			return next(ctx, req)
		}
	}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
