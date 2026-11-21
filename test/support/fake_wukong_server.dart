import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// Minimal JSON-RPC WebSocket server that mimics the WuKongIM v2.2.x wire
/// format closely enough to exercise the client's connection state machine.
class FakeWuKongServer {
  late final HttpServer _http;
  final sockets = <WebSocket>[];
  final frames = <Map<String, dynamic>>[];
  final _frames = StreamController<Map<String, dynamic>>.broadcast();

  /// CONNACK result for the n-th connection (0-based). Return null to leave
  /// the CONNECT unanswered, as WuKongIM does when token verification fails.
  Map<String, dynamic>? Function(int connection) connack = (_) => {
        'timeDiff': 1,
        'reasonCode': 1,
        'serverVersion': 5,
        'nodeId': 1,
      };

  /// Whether to answer PING on the n-th connection (0-based).
  bool Function(int connection) answerPing = (_) => true;

  /// Close the socket this long after an unanswered CONNECT (WuKongIM closes
  /// unauthenticated connections after its 4s idle limit).
  Duration? closeUnansweredConnectAfter;

  String get url => 'ws://${_http.address.address}:${_http.port}';

  Future<void> start() async {
    _http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _http.transform(WebSocketTransformer()).listen((socket) {
      final index = sockets.length;
      sockets.add(socket);
      socket.listen((data) {
        final frame = jsonDecode(data as String) as Map<String, dynamic>;
        frames.add(frame);
        _frames.add(frame);
        switch (frame['method']) {
          case 'connect':
            final result = connack(index);
            if (result != null) {
              socket.add(jsonEncode(
                  {'jsonrpc': '2.0', 'id': frame['id'], 'result': result}));
            } else if (closeUnansweredConnectAfter != null) {
              Timer(closeUnansweredConnectAfter!, socket.close);
            }
            break;
          case 'ping':
            // WuKongIM answers PING with a response that has no result.
            if (answerPing(index)) {
              socket.add(jsonEncode({'jsonrpc': '2.0', 'id': frame['id']}));
            }
            break;
        }
      }, onError: (_) {});
    });
  }

  int count(String method) => frames.where((f) => f['method'] == method).length;

  Future<Map<String, dynamic>> next(String method,
          {Duration timeout = const Duration(seconds: 5)}) =>
      _frames.stream.firstWhere((f) => f['method'] == method).timeout(timeout);

  /// Sends a JSON-RPC notification on the most recent connection.
  void notify(String method, Map<String, dynamic> params) {
    sockets.last.add(
        jsonEncode({'jsonrpc': '2.0', 'method': method, 'params': params}));
  }

  Future<void> close() async {
    for (final socket in sockets) {
      await socket.close();
    }
    await _http.close(force: true);
    await _frames.close();
  }
}
