import "dart:async";
import "dart:convert";

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

    test("throws SseLinkServerException on non-2xx status", () async {
      final client = _FakeClient(
        (_) async => _eventStream(
          Stream.fromIterable([_utf8("")]),
          status: 500,
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
              .having((e) => e.statusCode, "statusCode", 500),
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
}
