import "dart:async";
import "dart:convert";
import "dart:math";

import "package:gql_exec/gql_exec.dart";
import "package:gql_link/gql_link.dart";
import "package:http/http.dart" as http;
import "package:meta/meta.dart";

import "./exceptions.dart";

/// Controls how long to wait before the `retries`-th reconnection attempt.
///
/// `retries` counts actual (re)connection attempts and begins at `0` for the
/// first reconnect after a healthy connection drops. The returned future
/// completes when the wait is over.
///
/// See [SseLink.randomizedExponentialBackoff] for the default.
typedef RetryWait = Future<void> Function(int retries);

/// Decides whether a transport-level failure is worth reconnecting for.
///
/// Return `true` to reconnect (transient: network blip, 5xx, dropped stream)
/// or `false` to surface the failure as a terminal error on the response
/// stream (deterministic: 4xx, malformed payload, bad request).
///
/// See [SseLink.shouldRetryDefault] for the default.
typedef ShouldRetry = bool Function(Object error);

/// Produces extra HTTP headers to attach to each (re)connection.
///
/// Called on **every** connection attempt — including reconnects — so the
/// returned headers are always fresh. This is the SSE analog of
/// `gql_websocket_link`'s `connectionParams`: because SSE authenticates over
/// HTTP headers rather than a `connection_init` payload, it returns headers.
/// Use it to supply a bearer token that may have been refreshed since the
/// subscription first opened. The result may be computed asynchronously.
///
/// Returned headers take precedence over both the link's `defaultHeaders`
/// and the request's `HttpLinkHeaders` context entry.
typedef ConnectionParams = FutureOr<Map<String, String>?> Function();

/// A terminating [Link] that carries GraphQL subscriptions over
/// Server-Sent Events using the graphql-sse
/// ["distinct connections" mode](https://github.com/enisdenjo/graphql-sse/blob/master/PROTOCOL.md#distinct-connections-mode).
///
/// Each call to [request] opens one `POST` with
/// `Accept: text/event-stream`. Every `event: next` frame is decoded as
/// an [ExecutionResult] and yielded on the response stream. An
/// `event: complete` frame closes the stream.
///
/// This link is designed to be composed via [Link.split] so that only
/// subscription operations are routed to it; queries and mutations
/// should be forwarded to an HTTP link:
///
/// ```dart
/// final link = Link.split(
///   (request) => request.isSubscription,
///   SseLink("https://example.com/graphql/stream"),
///   HttpLink("https://example.com/graphql"),
/// );
/// ```
///
/// Provide your own [http.Client] to get HTTP/2 or HTTP/3 transport
/// (e.g. `cronet_http` on Android, `cupertino_http` on iOS).
///
/// ## Transparent reconnection
///
/// Subscriptions are long-lived, so the connection may drop mid-stream
/// (the OS stalls the app/tab, the server closes the stream, or the
/// network blips). Rather than surfacing such a drop as an error, the
/// link transparently re-issues the `POST` in the background with
/// exponential backoff, so consumers see one continuous response stream.
///
/// The reconnection policy is fully configurable and modelled on
/// `gql_websocket_link`:
///
/// - [retryAttempts] — how many consecutive reconnects to make before
///   giving up. Defaults to [unlimitedRetries] (effectively unbounded),
///   which suits live subscriptions.
/// - [retryWait] — the backoff schedule. Defaults to
///   [randomizedExponentialBackoff].
/// - [shouldRetry] — classifies a transport failure as transient
///   (reconnect) or deterministic (propagate). Defaults to
///   [shouldRetryDefault].
/// - [retryHealthyThreshold] — once a connection has stayed open at least
///   this long, the next drop resets the backoff to its first step.
/// - [connectionParams] — resolves extra headers (e.g. a refreshed auth
///   token) on every (re)connection.
///
/// An explicit `event: complete` is always terminal and never triggers a
/// reconnect. Deterministic failures (HTTP 4xx, malformed payloads,
/// request-format/context errors) propagate as errors instead of spinning
/// forever on a permanent condition.
///
/// > `Last-Event-ID` resumption is intentionally **not** implemented:
/// > distinct-connections mode re-executes the subscription from scratch,
/// > and these subscriptions stream full current state on every event, so
/// > a plain re-`POST` is sufficient.
class SseLink extends Link {
  /// Sentinel for [retryAttempts] meaning "reconnect indefinitely".
  static const int unlimitedRetries = -1;

  /// Endpoint of the GraphQL-over-SSE service.
  final Uri uri;

  /// Headers that are sent with every request.
  final Map<String, String> defaultHeaders;

  /// Serializer used to serialize the request.
  final RequestSerializer serializer;

  /// Parser used to parse each `next` event into a [Response].
  final ResponseParser parser;

  /// Maximum number of consecutive reconnection attempts before the
  /// response stream errors out. [unlimitedRetries] (the default) never
  /// gives up. The counter resets after a connection has stayed healthy
  /// for [retryHealthyThreshold].
  final int retryAttempts;

  /// The backoff schedule: awaited before each reconnect. `retries`
  /// starts at `0` for the first reconnect after a healthy connection.
  final RetryWait retryWait;

  /// Classifies a transport failure as transient (reconnect) or
  /// deterministic (propagate as a terminal error).
  final ShouldRetry shouldRetry;

  /// Once a connection has stayed open at least this long, the next drop
  /// is treated as "the first" and resets [retryWait]'s `retries` to `0`.
  final Duration retryHealthyThreshold;

  /// Optional per-connection header provider, resolved fresh on every
  /// (re)connection. Its headers override [defaultHeaders] and the request's
  /// context headers — use it to refresh an auth token across reconnects.
  final ConnectionParams? connectionParams;

  final http.Client _httpClient;
  final bool _ownsClient;
  final Set<_SseSubscription> _active = <_SseSubscription>{};

  /// Construct the link.
  ///
  /// Pass a custom [httpClient] to customize the transport (e.g. to get
  /// HTTP/2 or HTTP/3) or to add authentication. If no client is
  /// provided, a default [http.Client] is created and will be closed
  /// when [dispose] is called.
  ///
  /// The reconnection behaviour is tuned via [retryAttempts], [retryWait],
  /// [shouldRetry] and [retryHealthyThreshold]; see the class docs. Pass
  /// [connectionParams] to inject headers (e.g. a refreshed auth token) on
  /// every (re)connection.
  SseLink(
    String uri, {
    this.defaultHeaders = const {},
    http.Client? httpClient,
    this.serializer = const RequestSerializer(),
    this.parser = const ResponseParser(),
    this.retryAttempts = unlimitedRetries,
    RetryWait? retryWait,
    ShouldRetry? shouldRetry,
    this.retryHealthyThreshold = const Duration(seconds: 30),
    this.connectionParams,
  })  : uri = Uri.parse(uri),
        _httpClient = httpClient ?? http.Client(),
        _ownsClient = httpClient == null,
        retryWait = retryWait ?? randomizedExponentialBackoff,
        shouldRetry = shouldRetry ?? shouldRetryDefault;

  bool get _unlimitedRetries => retryAttempts < 0;

  @override
  Stream<Response> request(Request request, [NextLink? forward]) {
    final controller = StreamController<Response>();
    final subscription = _SseSubscription(this, request, controller);
    controller
      ..onListen = subscription.start
      ..onCancel = subscription.cancel;
    return controller.stream;
  }

  Response _parseNext(String data) {
    Map<String, dynamic> payload;
    try {
      payload = json.decode(data) as Map<String, dynamic>;
    } catch (e, s) {
      throw SseLinkParserException(
        originalException: e,
        originalStackTrace: s,
        data: data,
      );
    }
    Response response;
    try {
      response = parser.parseResponse(payload);
    } catch (e, s) {
      throw SseLinkParserException(
        originalException: e,
        originalStackTrace: s,
        data: data,
      );
    }
    if (response.data == null && response.errors == null) {
      throw SseLinkServerException(parsedResponse: response);
    }
    return response;
  }

  String _encodeBody(Request request) {
    try {
      return json.encode(serializer.serializeRequest(request));
    } catch (e, s) {
      throw RequestFormatException(
        originalException: e,
        originalStackTrace: s,
        request: request,
      );
    }
  }

  Map<String, String> _contextHeaders(Request request) {
    try {
      final HttpLinkHeaders? headers = request.context.entry();
      return headers?.headers ?? const <String, String>{};
    } catch (e, s) {
      throw ContextReadException(
        originalException: e,
        originalStackTrace: s,
      );
    }
  }

  @override
  Future<void> dispose() async {
    // Cancel any in-flight subscriptions before tearing down the client so
    // the transport does not surface aborted-stream errors to the zone.
    for (final subscription in _active.toList()) {
      await subscription.cancel();
    }
    if (_ownsClient) {
      _httpClient.close();
    }
  }

  /// The default [retryWait]: exponential backoff, capped, with jitter.
  ///
  /// Starts at a 1s base delay and doubles per attempt up to a 30s cap,
  /// then adds a random 300ms–3s jitter so reconnecting clients do not
  /// stampede the server. Mirrors `gql_websocket_link`'s
  /// `randomizedExponentialBackoff` but adds the cap.
  static Future<void> randomizedExponentialBackoff(int retries) async {
    const int baseDelayMs = 1000;
    const int maxDelayMs = 30000;
    var retryDelay = baseDelayMs;
    for (var i = 0; i < retries && retryDelay < maxDelayMs; i++) {
      retryDelay *= 2;
    }
    if (retryDelay > maxDelayMs) {
      retryDelay = maxDelayMs;
    }
    await Future<void>.delayed(
      Duration(
        milliseconds: retryDelay +
            // add a random 300ms–3s jitter
            (_random.nextDouble() * (3000 - 300) + 300).floor(),
      ),
    );
  }

  /// The default [shouldRetry]: reconnect on transient transport failures,
  /// but not on deterministic ones that would only repeat.
  ///
  /// Does not retry:
  /// - [SseLinkParserException] — malformed `next` payload.
  /// - [RequestFormatException] — the request could not be serialized.
  /// - [ContextReadException] — the request context is broken.
  /// - [SseLinkServerException] with a 4xx status — a client error
  ///   (e.g. 400/401/403) that a re-`POST` cannot fix.
  ///
  /// Retries everything else, notably network errors, dropped streams,
  /// and 5xx server errors.
  static bool shouldRetryDefault(Object error) {
    if (error is SseLinkParserException) return false;
    if (error is RequestFormatException) return false;
    if (error is ContextReadException) return false;
    if (error is SseLinkServerException) {
      final code = error.statusCode;
      if (code != null && code >= 400 && code < 500) return false;
      return true;
    }
    return true;
  }

  static final Random _random = Random();
}

/// Drives one [SseLink.request] response stream: opens the `POST`, forwards
/// `next`/`complete` frames, and transparently reconnects on transient
/// drops. One instance per active subscription.
class _SseSubscription {
  _SseSubscription(this._link, this._request, this._controller);

  final SseLink _link;
  final Request _request;
  final StreamController<Response> _controller;

  /// Subscription to the current connection's line stream, if connected.
  StreamSubscription<String>? _lineSub;

  /// True once the consumer cancelled or the stream reached a terminal
  /// state; guards against reconnects and double-close.
  bool _closed = false;

  /// True while a connection attempt is in flight; guards concurrent
  /// reconnects.
  bool _connecting = false;

  /// Count of consecutive failed attempts; feeds [SseLink.retryWait] and is
  /// reset once a connection stays healthy for [SseLink.retryHealthyThreshold].
  int _attempt = 0;

  /// Fires once the current connection has stayed open for
  /// [SseLink.retryHealthyThreshold]; resets [_attempt] so a later drop
  /// restarts the backoff from its first step.
  Timer? _healthyTimer;

  /// Memoized request body; a serialization failure is deterministic.
  String? _body;

  void start() {
    _link._active.add(this);
    unawaited(_connect());
  }

  /// Cancels the active HTTP subscription, swallowing any teardown error
  /// (e.g. `ClientException: Error in input stream` from aborting cronet or
  /// the fetch client) so nothing escapes to the zone.
  Future<void> cancel() async {
    _closed = true;
    _link._active.remove(this);
    _healthyTimer?.cancel();
    _healthyTimer = null;
    final sub = _lineSub;
    _lineSub = null;
    await sub?.cancel().catchError((_) {});
  }

  Future<void> _connect() async {
    if (_closed || _connecting) return;
    _connecting = true;

    // Resolve per-connection headers first; a failure here (e.g. a token
    // refresh that could not reach its endpoint) is transient by default and
    // is classified by [SseLink.shouldRetry] on its raw error.
    Map<String, String> dynamicHeaders = const <String, String>{};
    try {
      final params = await _link.connectionParams?.call();
      if (params != null) {
        dynamicHeaders = params;
      }
    } catch (error) {
      _connecting = false;
      _onDrop(error);
      return;
    }

    if (_closed) {
      _connecting = false;
      return;
    }

    final http.Request httpRequest;
    try {
      httpRequest = http.Request("POST", _link.uri)
        ..body = _body ??= _link._encodeBody(_request)
        ..headers.addAll(<String, String>{
          "Content-Type": "application/json",
          "Accept": "text/event-stream",
          ..._link.defaultHeaders,
          ..._link._contextHeaders(_request),
          ...dynamicHeaders,
        });
    } catch (error, stack) {
      // RequestFormatException / ContextReadException are deterministic.
      _connecting = false;
      _fail(error, stack);
      return;
    }

    final http.StreamedResponse response;
    try {
      response = await _link._httpClient.send(httpRequest);
    } catch (error, stack) {
      // Could not reach the server: transient by default.
      _connecting = false;
      _onDrop(SseLinkServerException(
        originalException: error,
        originalStackTrace: stack,
      ));
      return;
    }

    if (_closed) {
      unawaited(response.stream.drain<void>().catchError((_) {}));
      _connecting = false;
      return;
    }

    if (response.statusCode >= 300) {
      await response.stream.drain<void>().catchError((_) {});
      _connecting = false;
      // 4xx is deterministic; 5xx is transient — [shouldRetry] decides.
      _onDrop(SseLinkServerException(statusCode: response.statusCode));
      return;
    }

    // Connected: once the connection survives long enough, reset the
    // backoff so a later drop restarts from the first step.
    _healthyTimer?.cancel();
    _healthyTimer = Timer(_link.retryHealthyThreshold, () {
      _attempt = 0;
    });
    final framer = _SseFramer();
    var settled = false;

    _lineSub = response.stream
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(
      (line) {
        final event = framer.addLine(line);
        if (event == null) return;
        switch (event.event) {
          case "next":
            final Response parsed;
            try {
              parsed = _link._parseNext(event.data);
            } catch (error, stack) {
              // Malformed payload is deterministic → terminal.
              settled = true;
              _fail(error, stack);
              return;
            }
            _controller.add(parsed);
            break;
          case "complete":
            settled = true;
            _complete();
            break;
          default:
            break;
        }
      },
      onError: (Object error, StackTrace stack) {
        if (settled || _closed) return;
        settled = true;
        // Stream errored mid-flight → transient drop.
        _onDrop(SseLinkServerException(
          originalException: error,
          originalStackTrace: stack,
        ));
      },
      onDone: () {
        if (settled || _closed) return;
        settled = true;
        // Premature end with no `complete` frame → transient drop.
        _onDrop(null);
      },
      cancelOnError: true,
    );

    _connecting = false;
  }

  /// Handles a lost connection: propagate deterministic failures, otherwise
  /// schedule a reconnect (respecting [SseLink.retryAttempts]).
  void _onDrop(Object? error) {
    _healthyTimer?.cancel();
    _healthyTimer = null;
    final sub = _lineSub;
    _lineSub = null;
    sub?.cancel().catchError((_) {});

    if (_closed) return;

    if (error != null && !_link.shouldRetry(error)) {
      _fail(error, null);
      return;
    }

    if (!_link._unlimitedRetries && _attempt >= _link.retryAttempts) {
      // Out of attempts: surface the last error (or a generic one).
      _fail(error ?? const SseLinkServerException(), null);
      return;
    }

    final retries = _attempt;
    _attempt++;
    _link.retryWait(retries).then((_) {
      if (_closed) return;
      unawaited(_connect());
    });
  }

  /// Terminal error: propagate to the consumer and close.
  void _fail(Object error, StackTrace? stack) {
    if (_closed) return;
    _closed = true;
    _link._active.remove(this);
    _healthyTimer?.cancel();
    _healthyTimer = null;
    final sub = _lineSub;
    _lineSub = null;
    sub?.cancel().catchError((_) {});
    if (!_controller.isClosed) {
      _controller.addError(error, stack);
      unawaited(_controller.close());
    }
  }

  /// Terminal success: the server sent `event: complete`.
  void _complete() {
    if (_closed) return;
    _closed = true;
    _link._active.remove(this);
    _healthyTimer?.cancel();
    _healthyTimer = null;
    final sub = _lineSub;
    _lineSub = null;
    sub?.cancel().catchError((_) {});
    if (!_controller.isClosed) {
      unawaited(_controller.close());
    }
  }
}

/// A single parsed Server-Sent Event.
@immutable
@visibleForTesting
class SseEvent {
  final String event;
  final String data;

  const SseEvent(this.event, this.data);
}

/// Incremental Server-Sent Events framer.
///
/// Feed it one decoded line at a time via [addLine]; it returns a
/// [SseEvent] whenever a blank line completes one, following the WHATWG
/// SSE spec: concatenate multiple `data:` lines with `\n`, ignore comment
/// lines (beginning with `:`), and dispatch an event on every blank line.
/// Events without a `data:` field or an explicit `event:` name are skipped.
class _SseFramer {
  String _eventType = "message";
  final StringBuffer _dataBuffer = StringBuffer();
  bool _hasData = false;
  bool _hasField = false;

  /// Consumes [line], returning a completed [SseEvent] or `null`.
  SseEvent? addLine(String line) {
    if (line.isEmpty) {
      SseEvent? event;
      if (_hasField) {
        event = SseEvent(_eventType, _dataBuffer.toString());
      }
      _eventType = "message";
      _dataBuffer.clear();
      _hasData = false;
      _hasField = false;
      return event;
    }
    if (line.startsWith(":")) {
      return null;
    }
    final String field;
    final String value;
    final colonIdx = line.indexOf(":");
    if (colonIdx == -1) {
      field = line;
      value = "";
    } else {
      field = line.substring(0, colonIdx);
      final raw = line.substring(colonIdx + 1);
      value = raw.startsWith(" ") ? raw.substring(1) : raw;
    }
    switch (field) {
      case "event":
        _eventType = value;
        _hasField = true;
        break;
      case "data":
        if (_hasData) {
          _dataBuffer.write("\n");
        }
        _dataBuffer.write(value);
        _hasData = true;
        _hasField = true;
        break;
      default:
        break;
    }
    return null;
  }
}

/// Parses a raw UTF-8 byte stream of `text/event-stream` into
/// discrete [SseEvent]s according to the WHATWG SSE spec.
///
/// Concatenates multiple `data:` lines with `\n`, ignores comments
/// (lines beginning with `:`), and dispatches an event on every blank
/// line. Events without a `data:` field or an explicit `event:` name
/// are skipped.
@visibleForTesting
Stream<SseEvent> parseSseStream(Stream<List<int>> bytes) =>
    _parseSseStream(bytes);

Stream<SseEvent> _parseSseStream(Stream<List<int>> bytes) async* {
  final framer = _SseFramer();
  final lines = bytes.transform(utf8.decoder).transform(const LineSplitter());
  await for (final line in lines) {
    final event = framer.addLine(line);
    if (event != null) {
      yield event;
    }
  }
}
