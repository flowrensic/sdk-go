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

// EnvKeyHeader is the header every ingest call carries the env's access key in.
const EnvKeyHeader = "Flowrensic-Env-Key"

// DefaultTimeout is how long the hello may take when Options carries no
// Timeout.
const DefaultTimeout = 5 * time.Second

// Options is what a worker tells Flowrensic about itself.
type Options struct {
	// BaseURL is the Flowrensic server, such as http://flowrensic:8080.
	BaseURL string
	// EnvKey is the access key of the env, sent on every ingest call.
	EnvKey string
	// DeployEnv is the env this worker runs in, as the Flowrensic env list
	// names it.
	DeployEnv string
	// EnvInstanceID is which run of that env this is. Empty, it has none.
	EnvInstanceID string
	// WorkerID names this worker among the workers of the env instance.
	// Empty, the host name and the process id joined by a dash.
	WorkerID string
	// Version is the version this worker serves. Empty, none.
	Version string
	// Timeout is how long the hello may take. Zero, DefaultTimeout.
	Timeout time.Duration
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

// Connect says hello to the Flowrensic server, so the env instance and the
// worker show up there, and returns the client. A refused hello or one with
// no answer returns the error, whose code connect.CodeOf reads.
func Connect(ctx context.Context, options Options) (*Client, error) {
	if options.BaseURL == "" {
		return nil, errors.New("flowrensic: BaseURL is required")
	}
	if options.WorkerID == "" {
		options.WorkerID = defaultWorkerID()
	}
	if options.Timeout <= 0 {
		options.Timeout = DefaultTimeout
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	c := &Client{
		options: options,
		ingest: flowrensicv1connect.NewIngestServiceClient(
			httpClient,
			strings.TrimRight(options.BaseURL, "/"),
			connect.WithInterceptors(envKeyInterceptor(options.EnvKey)),
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
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	res, err := c.ingest.EnvInstanceHandshake(ctx, connect.NewRequest(&flowrensicv1.EnvInstanceHandshakeRequest{
		DeployEnv:     c.options.DeployEnv,
		EnvInstanceId: c.options.EnvInstanceID,
		WorkerId:      c.options.WorkerID,
		Version:       c.options.Version,
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

// envKeyInterceptor puts key on every call of IngestService. An empty key
// sends none.
func envKeyInterceptor(key string) connect.UnaryInterceptorFunc {
	ingest := "/" + flowrensicv1connect.IngestServiceName + "/"
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if key != "" && strings.HasPrefix(req.Spec().Procedure, ingest) {
				req.Header().Set(EnvKeyHeader, key)
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
