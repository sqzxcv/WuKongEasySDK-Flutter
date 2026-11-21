import 'package:flutter_test/flutter_test.dart';
import 'package:wukong_easy_sdk/src/core/event_manager.dart';
import 'package:wukong_easy_sdk/src/core/wukong_client.dart';
import 'package:wukong_easy_sdk/src/utils/wukong_logger.dart';
import 'package:wukong_easy_sdk/wukong_easy_sdk.dart';

import 'support/fake_wukong_server.dart';

/// Records every event a [WuKongClient] emits.
class Recorder {
  final EventManager events = EventManager();
  final connects = <ConnectResult>[];
  final disconnects = <DisconnectInfo>[];
  final errors = <WuKongError>[];
  final messages = <Message>[];

  Recorder() {
    events
      ..addEventListener<ConnectResult>(WuKongEvent.connect, connects.add)
      ..addEventListener<DisconnectInfo>(
          WuKongEvent.disconnect, disconnects.add)
      ..addEventListener<WuKongError>(WuKongEvent.error, errors.add)
      ..addEventListener<Message>(WuKongEvent.message, messages.add);
  }
}

Future<void> eventually(bool Function() condition,
    {Duration timeout = const Duration(seconds: 5)}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('condition not met within $timeout');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  late FakeWuKongServer server;
  late Recorder recorder;
  late WuKongClient client;

  WuKongClient newClient({
    Duration pingInterval = const Duration(seconds: 25),
    Duration pongTimeout = const Duration(seconds: 10),
  }) {
    client = WuKongClient(
      WuKongConfig(serverUrl: server.url, uid: 'u1-staff', token: 't'),
      recorder.events,
      const WuKongLogger(),
      pingInterval: pingInterval,
      pongTimeout: pongTimeout,
      initialReconnectDelay: const Duration(milliseconds: 20),
    );
    return client;
  }

  setUp(() async {
    server = FakeWuKongServer();
    await server.start();
    recorder = Recorder();
  });

  tearDown(() async {
    client.dispose();
    await server.close();
  });

  test('connects when the CONNACK omits timeDiff (Go omitempty for 0)',
      () async {
    server.connack = (_) => {'reasonCode': 1, 'serverVersion': 5, 'nodeId': 1};
    await newClient().connect();

    expect(client.isConnected, isTrue);
    expect(recorder.connects.single.timeDiff, 0);
  });
}
