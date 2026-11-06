# Flowrensic Go SDK

Pre-release. This package is an early preview and not ready for production. Its API can change in any version.

Flowrensic is a platform that runs beside Temporal. It watches what your workflows and workers do and tells you what went wrong, before and after a deploy.

## What it does today

- Connects a Temporal Go worker to your Flowrensic server, so the worker shows up in Flowrensic. Call client.Connect from go.flowrensic.com/sdk/client with ServerBaseURL, CurrentEnv, EnvAccessKey and CurrentEphemeralRun. Every call sends the env, its access key and the ephemeral run in the headers Flowrensic-Current-Env, Flowrensic-Env-Access-Key and Flowrensic-Current-Ephemeral-Run. The SDK reads no environment variable.
- An activity interceptor you can register on the worker: activity.NewInterceptor from go.flowrensic.com/sdk/activity, in worker.Options.Interceptors. For now it passes every call through unchanged.

## Planned

- Record each activity attempt: its result, error and time.
- Record the calls your app makes to Temporal, such as start, signal and query.
- Worker health: regular updates, so Flowrensic knows the worker is up.
- Replay before deploy: fetch real histories from Flowrensic, replay them against new code, and report the result.
- Deploy reports from CI.

The Node SDK, @flowrensic/node, already has these.

## Install

```sh
go get go.flowrensic.com/sdk
```
