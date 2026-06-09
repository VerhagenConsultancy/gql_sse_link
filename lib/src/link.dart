import "dart:async";
import "dart:convert";

import "package:gql_exec/gql_exec.dart";
import "package:gql_link/gql_link.dart";
import "package:http/http.dart" as http;
import "package:meta/meta.dart";

import "./exceptions.dart";

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
class SseLink extends Link {
  /// Endpoint of the GraphQL-over-SSE service.
  final Uri uri;

  /// Headers that are sent with every request.
  final Map<String, String> defaultHeaders;

  /// Serializer used to serialize the request.
  final RequestSerializer serializer;

  /// Parser used to parse each `next` event into a [Response].
  final ResponseParser parser;

  final http.Client _httpClient;
  final bool _ownsClient;

  /// Construct the link.
  ///
  /// Pass a custom [httpClient] to customize the transport (e.g. to get
  /// HTTP/2 or HTTP/3) or to add authentication. If no client is
  /// provided, a default [http.Client] is created and will be closed
  /// when [dispose] is called.
  SseLink(
    String uri, {
    this.defaultHeaders = const {},
    http.Client? httpClient,
    this.serializer = const RequestSerializer(),
    this.parser = const ResponseParser(),
  })  : uri = Uri.parse(uri),
        _httpClient = httpClient ?? http.Client(),
        _ownsClient = httpClient == null;

  @override
  Stream<Response> request(Request request, [NextLink? forward]) async* {
    final body = _encodeBody(request);
    final httpRequest = http.Request("POST", uri)
      ..body = body
      ..headers.addAll(<String, String>{
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        ...defaultHeaders,
        ..._contextHeaders(request),
      });

    final http.StreamedResponse response;
    try {
      response = await _httpClient.send(httpRequest);
    } catch (e, s) {
      throw SseLinkServerException(
        originalException: e,
        originalStackTrace: s,
      );
    }

    if (response.statusCode >= 300) {
      await response.stream.drain<void>();
      throw SseLinkServerException(statusCode: response.statusCode);
    }

    await for (final event in _parseSseStream(response.stream)) {
      switch (event.event) {
        case "next":
          yield _parseNext(event.data);
          break;
        case "complete":
          return;
        default:
          break;
      }
    }
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
    if (_ownsClient) {
      _httpClient.close();
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
  final lines = bytes.transform(utf8.decoder).transform(const LineSplitter());
  var eventType = "message";
  final dataBuffer = StringBuffer();
  var hasData = false;
  var hasField = false;

  await for (final line in lines) {
    if (line.isEmpty) {
      if (hasField) {
        yield SseEvent(eventType, dataBuffer.toString());
      }
      eventType = "message";
      dataBuffer.clear();
      hasData = false;
      hasField = false;
      continue;
    }
    if (line.startsWith(":")) {
      continue;
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
        eventType = value;
        hasField = true;
        break;
      case "data":
        if (hasData) {
          dataBuffer.write("\n");
        }
        dataBuffer.write(value);
        hasData = true;
        hasField = true;
        break;
      default:
        break;
    }
  }
}
