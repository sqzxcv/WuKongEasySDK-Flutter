import 'dart:convert';

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

  test(
      'a failed connect surfaces through connect() only, not as an '
      'uncaught async error', () async {
    // WuKongIM v2.2.4 never answers a CONNECT with a wrong token; it closes
    // the socket after its idle limit.
    server.connack = (_) => null;
    server.closeUnansweredConnectAfter = const Duration(milliseconds: 50);

    await expectLater(newClient().connect(), throwsA(isA<Exception>()));
    // An orphaned failed Future is reported to the zone asynchronously and
    // would fail this test.
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(client.isConnected, isFalse);
  });

  test('delivers and acknowledges RECV without a header', () async {
    await newClient().connect();
    final payload = base64Encode(utf8.encode(jsonEncode({
      'type': 1000,
      'staff_uid': 's1-staff',
    })));
    // WuKongIM omits "header" when noPersist/redDot/syncOnce/dup are all
    // false, e.g. system notices sent without a header or a user's own
    // messages synced to another device (redDot is cleared for the sender).
    server.notify('recv', {
      'messageId': '42',
      'messageSeq': 7,
      'timestamp': 1,
      'channelId': 'conv_v1-vtr',
      'channelType': 2,
      'fromUid': '',
      'payload': payload,
    });

    final ack = await server.next('recvack');
    expect(recorder.messages.single.messageSeq, 7);
    expect(recorder.messages.single.header.redDot, isNull);
    expect(ack['params']['messageId'], '42');
    expect(ack['params']['messageSeq'], 7);
  });

  test('reconnects after a ping timeout on a half-open connection', () async {
    // The first connection stops answering pings, like a network that drops
    // packets without closing the socket.
    server.answerPing = (connection) => connection > 0;
    await newClient(
      pingInterval: const Duration(milliseconds: 50),
      pongTimeout: const Duration(milliseconds: 50),
    ).connect();

    await eventually(() => server.count('connect') >= 2 && client.isConnected);
    expect(recorder.errors.map((e) => e.message), contains('Ping timeout'));
    expect(recorder.disconnects.map((d) => d.code), contains(1006));
    expect(recorder.connects, hasLength(2));
  });

  test('keeps reconnecting after repeated ping timeouts', () async {
    server.answerPing = (connection) => connection >= 2;
    await newClient(
      pingInterval: const Duration(milliseconds: 50),
      pongTimeout: const Duration(milliseconds: 50),
    ).connect();

    await eventually(() => server.count('connect') >= 3 && client.isConnected);
    expect(recorder.connects, hasLength(3));
  });

  test('a server DISCONNECT (kick) is terminal and reported once', () async {
    await newClient().connect();
    // WuKongIM sends DISCONNECT and closes the socket 2s (connect kick) or
    // 10s (token update) later.
    server.notify('disconnect', {
      'reasonCode': WuKongReasonCode.connectKick.value,
      'reason': 'login in other device',
    });
    await eventually(() => recorder.disconnects.isNotEmpty);
    await server.sockets.single.close();
    await Future<void>.delayed(const Duration(milliseconds: 200));

    expect(client.isConnected, isFalse);
    expect(recorder.disconnects, hasLength(1));
    expect(recorder.disconnects.single.code, 12);
    expect(recorder.disconnects.single.reason, 'login in other device');
    expect(server.count('connect'), 1, reason: 'must not auto-reconnect');
  });

  test('an explicit connect() after a kick connects again', () async {
    await newClient().connect();
    server.notify('disconnect', {'reasonCode': 12});
    await eventually(() => recorder.disconnects.isNotEmpty);

    await client.connect();
    expect(client.isConnected, isTrue);
    expect(server.count('connect'), 2);
  });

  test('manual disconnect cancels a pending automatic reconnect', () async {
    server.answerPing = (_) => false;
    await newClient(
      pingInterval: const Duration(milliseconds: 50),
      pongTimeout: const Duration(milliseconds: 50),
    ).connect();
    await eventually(
        () => recorder.errors.any((e) => e.message == 'Ping timeout'));
    client.disconnect();
    final connectsAtDisconnect = server.count('connect');
    await Future<void>.delayed(const Duration(milliseconds: 300));

    expect(client.isConnected, isFalse);
    expect(server.count('connect'), connectsAtDisconnect);
  });
}
