## 1.0.0-beta.3

- **`connectionParams` callback** — an optional `FutureOr<Map<String, String>?>
  Function()?` resolved fresh on every (re)connection, letting you inject
  headers (typically a refreshed auth token) that override `defaultHeaders` and
  the request's context headers. The SSE analog of `gql_websocket_link`'s
  `connectionParams`. A failure resolving it is treated as a transient drop.

## 1.0.0-beta.2

- **Transparent reconnection.** A subscription whose stream drops mid-flight —
  the transport errors (e.g. `ClientException: Error in input stream`) or the
  stream ends with no `event: complete` — is now reconnected in the background
  by re-issuing the `POST`, so consumers see one continuous stream instead of a
  permanent failure. An explicit `event: complete` is still terminal, and
  deterministic failures (HTTP 4xx, malformed payloads, request-format/context
  errors) still propagate as errors.
- **Clean teardown.** Cancelling a subscription now aborts the in-flight HTTP
  response and swallows the resulting transport error, so nothing escapes to
  the zone.
- **Configurable retry policy** via new optional, backward-compatible
  constructor parameters modelled on `gql_websocket_link`: `retryAttempts`
  (defaults to `SseLink.unlimitedRetries`), `retryWait` (defaults to
  `SseLink.randomizedExponentialBackoff` — exponential with a 30s cap and
  jitter), `shouldRetry` (defaults to `SseLink.shouldRetryDefault`), and
  `retryHealthyThreshold` (resets the backoff after a connection stays healthy).
- `Last-Event-ID` resumption remains a documented non-goal: distinct-connections
  mode re-executes from scratch and subscriptions stream full current state, so
  a plain re-`POST` is sufficient.

## 1.0.0-beta.1

- Initial standalone release, extracted from the [`gql`](https://github.com/gql-dart/gql) project.
- Implements the `graphql-sse` "distinct connections mode" for GraphQL subscriptions over Server-Sent Events.
