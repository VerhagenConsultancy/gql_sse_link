import "dart:async";
import "dart:convert";

import "package:fake_async/fake_async.dart";
import "package:gql/language.dart";
import "package:gql_exec/gql_exec.dart";
import "package:gql_sse_link/gql_sse_link.dart";
import "package:http/http.dart" as http;
import "package:test/test.dart";

class _FakeClient extends http.BaseClient {
  _FakeClient(this.responder);

  final Future<http.StreamedResponse> Function(http.BaseRequest) responder;

  http.BaseRequest? lastRequest;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    lastRequest = request;
    return responder(request);
  }
}

/// A client that hands out a fresh, test-controllable byte stream on every
/// `send`, so a test can drive one connection at a time and simulate drops
/// and reconnections. Each element of [conns] is the [StreamController]
/// backing the corresponding connection's response body.
class _ReconnectClient extends http.BaseClient {
  _ReconnectClient({
    List<int>? statuses,
    this.onCancel,
  }) : _statuses = statuses ?? const <int>[200];

  /// HTTP status per connection; the last value repeats for further calls.
  final List<int> _statuses;

  /// Optional callback wired to each connection body's `onCancel`, used to
  /// simulate a transport that throws when its stream is aborted.
  final void Function()? onCancel;

  final List<StreamController<List<int>>> conns =
      <StreamController<List<int>>>[];

  /// The request sent for each connection, in order.
  final List<http.BaseRequest> requests = <http.BaseRequest>[];

  int get calls => conns.length;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    requests.add(request);
    final index = conns.length;
    final controller = StreamController<List<int>>();
    if (onCancel != null) {
      controller.onCancel = onCancel;
    }
    conns.add(controller);
    final status =
        _statuses[index < _statuses.length ? index : _statuses.length - 1];
    return http.StreamedResponse(
      controller.stream,
      status,
      headers: const {"content-type": "text/event-stream"},
    );
  }
}

/// Zero-delay [RetryWait] so reconnect tests advance on microtasks.
Future<void> _immediate(int retries) async {}

http.StreamedResponse _eventStream(
  Stream<List<int>> body, {
  int status = 200,
  Map<String, String> headers = const {"content-type": "text/event-stream"},
}) =>
    http.StreamedResponse(body, status, headers: headers);

List<int> _utf8(String s) => utf8.encode(s);

Request _subscription() => Request(
      operation: Operation(
        document: parseString(
          "subscription Hello { hello }",
        ),
      ),
    );

void main() {
  group("parseSseStream", () {
    test("dispatches an event on blank line", () async {
      final bytes = Stream.fromIterable([
        _utf8("event: next\ndata: {\"foo\":1}\n\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events, hasLength(1));
      expect(events.first.event, "next");
      expect(events.first.data, '{"foo":1}');
    });

    test("joins multiple data lines with newlines", () async {
      final bytes = Stream.fromIterable([
        _utf8("event: next\ndata: line1\ndata: line2\n\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events.first.data, "line1\nline2");
    });

    test("ignores comment lines", () async {
      final bytes = Stream.fromIterable([
        _utf8(": this is a comment\nevent: next\ndata: x\n\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events, hasLength(1));
      expect(events.first.data, "x");
    });

    test("handles split byte chunks", () async {
      final bytes = Stream.fromIterable([
        _utf8("event: ne"),
        _utf8("xt\ndata: {\"a\":"),
        _utf8("1}\n\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events.first.event, "next");
      expect(events.first.data, '{"a":1}');
    });

    test("emits multiple events separated by blank lines", () async {
      final bytes = Stream.fromIterable([
        _utf8("event: next\ndata: 1\n\nevent: next\ndata: 2\n\n"
            "event: complete\n\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events.map((e) => e.event).toList(),
          <String>["next", "next", "complete"]);
      expect(events.map((e) => e.data).toList(), <String>["1", "2", ""]);
    });

    test("does not emit trailing event without blank line", () async {
      final bytes = Stream.fromIterable([
        _utf8("event: next\ndata: 1\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events, isEmpty);
    });

    test("strips single leading space from value", () async {
      final bytes = Stream.fromIterable([
        _utf8("data:no-space\ndata: with-space\n\n"),
      ]);

      final events = await parseSseStream(bytes).toList();

      expect(events.first.data, "no-space\nwith-space");
    });
  });

  group("SseLink", () {
    test("sends POST with Accept: text/event-stream and serialized body",
        () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([
            _utf8("event: complete\n\n"),
          ]),
        ),
      );
      final link = SseLink(
        "https://example.com/graphql/stream",
        httpClient: client,
      );

      await link.request(_subscription()).drain<void>();

      final request = client.lastRequest! as http.Request;
      expect(request.method, "POST");
      expect(request.url.toString(), "https://example.com/graphql/stream");
      expect(request.headers["Accept"], "text/event-stream");
      expect(request.headers["Content-Type"], "application/json");
      final decoded = json.decode(request.body) as Map<String, dynamic>;
      expect(decoded["query"], contains("subscription Hello"));
    });

    test("yields parsed responses for each next event", () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([
            _utf8(
              'event: next\ndata: {"data":{"hello":"a"}}\n\n'
              'event: next\ndata: {"data":{"hello":"b"}}\n\n'
              "event: complete\n\n",
            ),
          ]),
        ),
      );
      final link = SseLink(
        "https://example.com/graphql/stream",
        httpClient: client,
      );

      final responses = await link.request(_subscription()).toList();

      expect(responses, hasLength(2));
      expect(responses[0].data, const <String, dynamic>{"hello": "a"});
      expect(responses[1].data, const <String, dynamic>{"hello": "b"});
    });

    test("closes the stream on complete event", () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([
            _utf8('event: next\ndata: {"data":{"hello":"a"}}\n\n'
                "event: complete\n\n"
                // anything after complete must be ignored
                'event: next\ndata: {"data":{"hello":"b"}}\n\n'),
          ]),
        ),
      );
      final link = SseLink(
        "https://example.com/graphql/stream",
        httpClient: client,
      );

      final responses = await link.request(_subscription()).toList();

      expect(responses, hasLength(1));
    });

    test("merges default headers and per-request HttpLinkHeaders", () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([
            _utf8("event: complete\n\n"),
          ]),
        ),
      );
      final link = SseLink(
        "https://example.com/graphql/stream",
        httpClient: client,
        defaultHeaders: const <String, String>{"X-Default": "yes"},
      );

      final request = _subscription().withContextEntry(
        const HttpLinkHeaders(headers: <String, String>{"X-Ctx": "also"}),
      );
      await link.request(request).drain<void>();

      final sent = client.lastRequest!;
      expect(sent.headers["X-Default"], "yes");
      expect(sent.headers["X-Ctx"], "also");
    });

    test("throws SseLinkServerException on 4xx status (no reconnect)",
        () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([_utf8("")]),
          status: 403,
        ),
      );
      final link = SseLink(
        "https://example.com/graphql/stream",
        httpClient: client,
      );

      expect(
        link.request(_subscription()).toList(),
        throwsA(
          isA<SseLinkServerException>()
              .having((e) => e.statusCode, "statusCode", 403),
        ),
      );
    });

    test("throws SseLinkParserException on malformed next payload", () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([
            _utf8("event: next\ndata: not-json\n\n"),
          ]),
        ),
      );
      final link = SseLink(
        "https://example.com/graphql/stream",
        httpClient: client,
      );

      expect(
        link.request(_subscription()).toList(),
        throwsA(isA<SseLinkParserException>()),
      );
    });
  });

  group("SseLink reconnection", () {
    List<int> event(String hello) => _utf8(
          'event: next\ndata: {"data":{"hello":"$hello"}}\n\n',
        );

    test("reconnects after the stream errors mid-subscription", () {
      fakeAsync((async) {
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
        );

        final responses = <Response>[];
        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              responses.add,
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        expect(client.calls, 1);

        client.conns[0].add(event("a"));
        async.flushMicrotasks();
        expect(responses, hasLength(1));

        // The transport errors mid-stream (e.g. cronet/fetch drop).
        client.conns[0].addError(http.ClientException("Error in input stream"));
        async.flushMicrotasks();

        // Reconnected transparently: no error surfaced, stream still open.
        expect(client.calls, 2);
        expect(error, isNull);
        expect(done, isFalse);

        client.conns[1].add(event("b"));
        client.conns[1].add(_utf8("event: complete\n\n"));
        async.flushMicrotasks();

        expect(responses.map((r) => r.data).toList(), <Object>[
          const <String, dynamic>{"hello": "a"},
          const <String, dynamic>{"hello": "b"},
        ]);
        expect(error, isNull);
        expect(done, isTrue);
      });
    });

    test("reconnects after the stream ends without a complete event", () {
      fakeAsync((async) {
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
        );

        final responses = <Response>[];
        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              responses.add,
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        client.conns[0].add(event("a"));
        async.flushMicrotasks();

        // Premature end: the body closes with no `complete` frame.
        client.conns[0].close();
        async.flushMicrotasks();

        expect(client.calls, 2);
        expect(error, isNull);
        expect(done, isFalse);

        client.conns[1].add(event("b"));
        client.conns[1].add(_utf8("event: complete\n\n"));
        async.flushMicrotasks();

        expect(responses, hasLength(2));
        expect(done, isTrue);
        expect(error, isNull);
      });
    });

    test("does not reconnect after an explicit complete event", () {
      fakeAsync((async) {
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
        );

        final responses = <Response>[];
        var done = false;
        link.request(_subscription()).listen(
              responses.add,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        client.conns[0].add(event("a"));
        client.conns[0].add(_utf8("event: complete\n\n"));
        async.flushMicrotasks();

        expect(responses, hasLength(1));
        expect(done, isTrue);

        // Give any (erroneous) reconnect ample time to fire.
        async.elapse(const Duration(minutes: 1));
        expect(client.calls, 1);
      });
    });

    test("reconnects on a 5xx status", () {
      fakeAsync((async) {
        final client = _ReconnectClient(statuses: <int>[503, 200]);
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
        );

        final responses = <Response>[];
        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              responses.add,
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        // The 503 body drains once closed, then the link retries.
        client.conns[0].close();
        async.flushMicrotasks();

        expect(client.calls, 2);
        expect(error, isNull);

        client.conns[1].add(event("a"));
        client.conns[1].add(_utf8("event: complete\n\n"));
        async.flushMicrotasks();

        expect(responses, hasLength(1));
        expect(done, isTrue);
        expect(error, isNull);
      });
    });

    test("does not reconnect on a 4xx status", () {
      fakeAsync((async) {
        final client = _ReconnectClient(statuses: <int>[401]);
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
        );

        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              (_) {},
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        client.conns[0].close();
        async.flushMicrotasks();

        expect(error, isA<SseLinkServerException>());
        expect((error! as SseLinkServerException).statusCode, 401);
        expect(done, isTrue);

        async.elapse(const Duration(minutes: 1));
        expect(client.calls, 1);
      });
    });

    test("does not reconnect on a malformed next payload", () {
      fakeAsync((async) {
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
        );

        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              (_) {},
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        client.conns[0].add(_utf8("event: next\ndata: not-json\n\n"));
        async.flushMicrotasks();

        expect(error, isA<SseLinkParserException>());
        expect(done, isTrue);

        async.elapse(const Duration(minutes: 1));
        expect(client.calls, 1);
      });
    });

    test("cancelling mid-stream surfaces no error to the zone", () {
      final zoneErrors = <Object>[];
      fakeAsync((async) {
        runZonedGuarded(
          () {
            final client = _ReconnectClient(
              onCancel: () => throw http.ClientException(
                "Error in input stream",
              ),
            );
            final link = SseLink(
              "https://example.com/graphql/stream",
              httpClient: client,
            );

            final sub = link.request(_subscription()).listen((_) {});
            async.flushMicrotasks();
            client.conns[0].add(event("a"));
            async.flushMicrotasks();

            // Cancelling aborts the in-flight body, whose onCancel throws.
            sub.cancel();
            async.flushMicrotasks();
          },
          (Object error, StackTrace stack) => zoneErrors.add(error),
        );
        async.elapse(const Duration(seconds: 1));
      });

      expect(zoneErrors, isEmpty);
    });

    test("backoff waits, counts retries, and resets after a healthy connection",
        () {
      fakeAsync((async) {
        final retries = <int>[];
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryHealthyThreshold: const Duration(seconds: 10),
          retryWait: (retry) async {
            retries.add(retry);
            await Future<void>.delayed(const Duration(seconds: 2));
          },
        );

        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              (_) {},
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        expect(client.calls, 1);

        // First drop: retryWait(0), then wait 2s before reconnecting.
        client.conns[0].close();
        async.flushMicrotasks();
        expect(retries, <int>[0]);
        expect(client.calls, 1);
        async.elapse(const Duration(seconds: 2));
        expect(client.calls, 2);

        // Second consecutive drop: retryWait(1) — backoff advances.
        client.conns[1].close();
        async.flushMicrotasks();
        expect(retries, <int>[0, 1]);
        async.elapse(const Duration(seconds: 2));
        expect(client.calls, 3);

        // Third connection stays healthy past the threshold, resetting
        // the backoff; the next drop therefore starts again at 0.
        async.elapse(const Duration(seconds: 11));
        client.conns[2].close();
        async.flushMicrotasks();
        expect(retries, <int>[0, 1, 0]);
        async.elapse(const Duration(seconds: 2));
        expect(client.calls, 4);

        expect(error, isNull);
        expect(done, isFalse);

        // Clean up the still-open connection's pending timers.
        client.conns[3].close();
        async.flushMicrotasks();
      });
    });

    test("gives up after retryAttempts consecutive failures", () {
      fakeAsync((async) {
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryAttempts: 2,
          retryWait: _immediate,
        );

        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              (_) {},
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();
        // Drop each connection immediately; 2 reconnects then give up.
        for (var i = 0; i < 4; i++) {
          if (i < client.calls) {
            client.conns[i].close();
          }
          async.flushMicrotasks();
        }

        // 1 initial + 2 reconnects = 3 connections, then it errors out.
        expect(client.calls, 3);
        expect(error, isA<SseLinkServerException>());
        expect(done, isTrue);
      });
    });

    test("applies connectionParams headers, refreshed on each reconnect", () {
      fakeAsync((async) {
        var token = "t1";
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
          defaultHeaders: const <String, String>{"Authorization": "stale"},
          connectionParams: () async =>
              <String, String>{"Authorization": "Bearer $token"},
        );

        final sub = link.request(_subscription()).listen((_) {});
        async.flushMicrotasks();

        // connectionParams overrides defaultHeaders on the first connection.
        expect(client.requests[0].headers["Authorization"], "Bearer t1");

        // Rotate the token, then force a reconnect via a premature end.
        token = "t2";
        client.conns[0].close();
        async.flushMicrotasks();

        expect(client.calls, 2);
        // The reconnect picks up the refreshed token.
        expect(client.requests[1].headers["Authorization"], "Bearer t2");

        sub.cancel();
      });
    });

    test("treats a connectionParams failure as a transient drop", () {
      fakeAsync((async) {
        var failNext = true;
        final client = _ReconnectClient();
        final link = SseLink(
          "https://example.com/graphql/stream",
          httpClient: client,
          retryWait: _immediate,
          connectionParams: () async {
            if (failNext) {
              failNext = false;
              throw http.ClientException("token fetch failed");
            }
            return null;
          },
        );

        Object? error;
        var done = false;
        link.request(_subscription()).listen(
              (_) {},
              onError: (Object e) => error = e,
              onDone: () => done = true,
            );

        async.flushMicrotasks();

        // The first attempt threw before sending, then retried and connected.
        expect(client.calls, 1);
        expect(error, isNull);

        client.conns[0].add(_utf8("event: complete\n\n"));
        async.flushMicrotasks();
        expect(done, isTrue);
        expect(error, isNull);
      });
    });
  });

  group("SseLink.randomizedExponentialBackoff", () {
    test("increases exponentially and caps the base delay", () {
      // Measure the fake time the returned future waits for.
      Duration waited(int retries) {
        late Duration result;
        fakeAsync((async) {
          final start = async.elapsed;
          SseLink.randomizedExponentialBackoff(retries).then((_) {
            result = async.elapsed - start;
          });
          async.elapse(const Duration(seconds: 120));
        });
        return result;
      }

      // Base delays: 1s, 2s, 4s, ... capped at 30s; jitter adds 0.3–3s.
      final r0 = waited(0);
      final r1 = waited(1);
      final r10 = waited(10);

      expect(r0.inMilliseconds, greaterThanOrEqualTo(1000));
      expect(r1.inMilliseconds, greaterThanOrEqualTo(2000));
      // Capped: base 30s + up to 3s jitter.
      expect(r10.inMilliseconds, greaterThanOrEqualTo(30000));
      expect(r10.inMilliseconds, lessThanOrEqualTo(33000));
    });
  });
}
