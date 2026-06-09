import "package:gql_link/gql_link.dart";
import "package:gql_sse_link/gql_sse_link.dart";

void main() {
  // ignore: unused_local_variable
  final link = Link.from([
    SseLink("https://example.com/graphql/stream"),
  ]);

  // In practice, compose with an HTTP link via `Link.split` so only
  // subscriptions hit the SSE endpoint:
  //
  // final link = Link.split(
  //   (request) =>
  //       request.operation.getOperationType() == OperationType.subscription,
  //   SseLink("https://example.com/graphql/stream"),
  //   HttpLink("https://example.com/graphql"),
  // );
}
