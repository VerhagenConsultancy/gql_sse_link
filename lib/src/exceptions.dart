import "package:gql_exec/gql_exec.dart";
import "package:gql_link/gql_link.dart";
import "package:meta/meta.dart";

/// Exception thrown when the SSE response cannot be parsed into a
/// GraphQL [Response].
@immutable
class SseLinkParserException extends ResponseFormatException {
  /// The raw `data:` payload that failed to parse.
  final String data;

  const SseLinkParserException({
    required Object? originalException,
    required StackTrace? originalStackTrace,
    required this.data,
  }) : super(
          originalException: originalException,
          originalStackTrace: originalStackTrace,
        );
}

/// Exception thrown when the SSE transport fails (non-2xx status,
/// network error, or a parsed response missing both `data` and `errors`).
@immutable
class SseLinkServerException extends ServerException {
  const SseLinkServerException({
    Object? originalException,
    StackTrace? originalStackTrace,
    Response? parsedResponse,
    int? statusCode,
  }) : super(
          originalException: originalException,
          originalStackTrace: originalStackTrace,
          parsedResponse: parsedResponse,
          statusCode: statusCode,
        );
}
