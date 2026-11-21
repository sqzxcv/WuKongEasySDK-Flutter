// TALK-7 验证程序：用 Easy SDK 以客服身份连 WuKongIM v2.2.4-20260313，
// 逐项验证连接、收发、踢下线、心跳重连等行为。
//
// 只连本机测试环境（futrtalk 主仓库 respos/futrtalk-flutter/tool/localenv）：
//   业务 API  http://127.0.0.1:10010
//   WuKongIM  API http://127.0.0.1:15001  WS ws://127.0.0.1:15002
//   netproxy  ws://127.0.0.1:25002 → 15002，控制口 http://127.0.0.1:29999
//
// 运行（在 example/ 目录下）：
//   FT_API_KEY=$(cat <futrtalk>/data/localenv/run/api_key) \
//     dart run tool/talk7_probe.dart <场景> [--staff 3] [--flag 0]
//
// localenv 的 WuKongIM 没开 tokenAuthOn（任何 token 都能连），测 token 错误、
// 刷新、顶号时另起一个开了鉴权的 WuKongIM（同一个二进制，换端口，配置里加
// `tokenAuthOn: true`），再用环境变量指过去，并跳过业务登录：
//   WK_API=http://127.0.0.1:16001 WK_WS=ws://127.0.0.1:16002 \
//   TALK7_STAFF_UID=<坐席 uid> dart run tool/talk7_probe.dart refresh
//
// 不是给官方仓库的代码：依赖 FutrTalk 的业务接口和本机测试环境。
//
// 场景：
//   basic     客服（--flag 设备类型）和访客互相收发文字，检查解码
//   dupe      同一个 clientMsgNo 发两次，数访客收到几条、服务端存了几条
//   badtoken  错误 token 连接，记录多久报错、报什么
//   refresh   已连接时用主设备级别更新 token（模拟刷新），记录旧连接的现象
//   kick      另一台同类型设备登录（更新 token + 新 deviceId 连上），记录旧连接的现象
//   connkick  同一 token、不同 deviceId 的第二条连接，记录旧连接的现象
//   coexist   同一客服 Web(1)、App(0)、Desktop(2) 同时在线，中途 Web 重新登录
//   listen    只连上并打印收到的所有消息（配合 staffbot.py 触发系统消息）
//   outage    经 netproxy 连接，/drop 持续 --seconds 秒后 /up，看 SDK 是否还会自动连回
//             （会调用 netproxy，运行前先拿设备锁）
//   connloop  反复连接、断开 --times 次，统计连接失败（timeDiff 为 0 时 v1.2.1 会失败）
//   heartbeat 经 netproxy 连接，/drop 半断开，等心跳超时，/up 后看能否自动连回
//             （会调用 netproxy，运行前先拿设备锁）
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:wukong_easy_sdk/wukong_easy_sdk.dart';

const apiBase = 'http://127.0.0.1:10010';
// WK_API / WK_WS 可以指向另一个开了 tokenAuthOn 的 WuKongIM（本机 localenv 没开）
final wkApi = Platform.environment['WK_API'] ?? 'http://127.0.0.1:15001';
final wsDirect = Platform.environment['WK_WS'] ?? 'ws://127.0.0.1:15002';
const wsProxied = 'ws://127.0.0.1:25002';
const netproxy = 'http://127.0.0.1:29999';
const password = 'Test1234!';

final _t0 = DateTime.now();
String _ts() {
  final ms = DateTime.now().difference(_t0).inMilliseconds;
  return '+${(ms / 1000).toStringAsFixed(2).padLeft(7)}s';
}

void log(String s) => stdout.writeln('${_ts()} $s');

Future<dynamic> http(
  String method,
  String url, {
  Object? body,
  String? bearer,
}) async {
  final client = HttpClient();
  try {
    final req = await client.openUrl(method, Uri.parse(url));
    req.headers.contentType = ContentType.json;
    if (bearer != null) req.headers.set('Authorization', 'Bearer $bearer');
    if (body != null) req.add(utf8.encode(jsonEncode(body)));
    final resp = await req.close();
    final text = await utf8.decodeStream(resp);
    if (resp.statusCode >= 300) {
      throw HttpException('$method $url -> ${resp.statusCode} $text');
    }
    return text.isEmpty ? null : jsonDecode(text);
  } finally {
    client.close();
  }
}

class Staff {
  final String uid, jwt, webImToken;
  Staff(this.uid, this.jwt, this.webImToken);
}

/// 业务登录：后端会以 Web(1) 主设备级别更新 im_token，踢掉该坐席现有的 Web 连接。
/// 设了 TALK7_STAFF_UID 时不登录，直接用这个 uid（连另一个 WuKongIM 时用）。
Future<Staff> staffLogin(int n) async {
  final uid = Platform.environment['TALK7_STAFF_UID'];
  if (uid != null && uid.isNotEmpty) return Staff(uid, '', '');
  final r = await http(
    'POST',
    '$apiBase/api/v2/login/account',
    body: {'email': 'staff$n@local.test', 'password': password},
  );
  final d = r['data'] as Map<String, dynamic>;
  return Staff(
    d['uuid'] as String,
    d['staff_token']['token'] as String,
    d['im_token']['token'] as String,
  );
}

/// 后端还没有 App/Desktop 设备类型的 token，直接调 WuKongIM 接口注册（主设备级别）。
Future<String> registerToken(String uid, int flag, {String? token}) async {
  final t = token ?? 'talk7-$flag-${DateTime.now().microsecondsSinceEpoch}';
  await http(
    'POST',
    '$wkApi/user/token',
    body: {'uid': uid, 'token': t, 'device_flag': flag, 'device_level': 1},
  );
  return t;
}

class Visitor {
  final String uid, imToken, channelId, jwt;
  Visitor(this.uid, this.imToken, this.channelId, this.jwt);
}

Future<Visitor> visitorLogin() async {
  final key = Platform.environment['FT_API_KEY'];
  if (key == null || key.isEmpty) {
    throw StateError('需要环境变量 FT_API_KEY（localenv 的 run/api_key）');
  }
  final r = await http(
    'POST',
    '$apiBase/api/v3/login/visitor_login',
    body: {
      'platform_api_key': key,
      'device': {
        'device_id': 'talk7-${DateTime.now().microsecondsSinceEpoch}',
        'device_model': 'talk7-probe',
        'os_version': '1',
        'app_version': '1',
        'platform': 'flutter',
      },
    },
  );
  final d = r['data'] as Map<String, dynamic>;
  return Visitor(
    d['visitor']['uuid'] as String,
    d['im_token']['token'] as String,
    d['channel_id'] as String,
    d['access_token']['token'] as String,
  );
}

/// 一条 Easy SDK 连接，记录所有事件。
class Peer {
  final String name;
  final WuKongEasySDK sdk = WuKongEasySDK.createInstance();
  final messages = <Message>[];
  final events = <String>[];
  final disconnects = <DisconnectInfo>[];
  final errors = <WuKongError>[];
  int connects = 0;
  final _waiters = <bool Function(), Completer<void>>{};

  Peer(this.name);

  Future<void> init({
    required String uid,
    required String token,
    required int flag,
    String? deviceId,
    String? url,
    bool verbose = false,
  }) async {
    await sdk.init(
      WuKongConfig(
        serverUrl: url ?? wsDirect,
        uid: uid,
        token: token,
        deviceId: deviceId ?? '$name-device',
        deviceFlag: WuKongDeviceFlag.fromValue(flag),
        debugLogging: verbose,
        logHandler: verbose ? (m) => log('[$name sdk] $m') : null,
      ),
    );
    sdk.addEventListener<ConnectResult>(WuKongEvent.connect, (r) {
      connects++;
      _ev('connect $r');
    });
    sdk.addEventListener<DisconnectInfo>(WuKongEvent.disconnect, (d) {
      disconnects.add(d);
      _ev('disconnect code=${d.code} reason=${d.reason}');
    });
    sdk.addEventListener<WuKongError>(WuKongEvent.error, (e) {
      errors.add(e);
      _ev('error ${e.code.name}: ${e.message}');
    });
    sdk.addEventListener<ReconnectingInfo>(WuKongEvent.reconnecting, (r) {
      _ev('reconnecting attempt=${r.attempt} delay=${r.delay}ms');
    });
    sdk.addEventListener<Message>(WuKongEvent.message, (m) {
      messages.add(m);
      final p = decodePayload(m.payload);
      _ev(
        'message from=${m.fromUid} ch=${m.channelId}/${m.channelType.value} '
        'seq=${m.messageSeq} cmn=${m.clientMsgNo} '
        'header=${jsonEncode(m.header.toJson())} payload=${jsonEncode(p)}',
      );
    });
  }

  void _ev(String s) {
    events.add('${_ts()} $s');
    log('[$name] $s');
    for (final e in _waiters.entries.toList()) {
      if (e.key()) {
        _waiters.remove(e.key);
        if (!e.value.isCompleted) e.value.complete();
      }
    }
  }

  Future<bool> waitFor(bool Function() cond, Duration timeout) async {
    if (cond()) return true;
    final c = Completer<void>();
    _waiters[cond] = c;
    try {
      await c.future.timeout(timeout);
      return true;
    } on TimeoutException {
      _waiters.remove(cond);
      return false;
    }
  }

  Future<void> connect() async {
    final sw = Stopwatch()..start();
    await sdk.connect();
    log('[$name] connect() ok in ${sw.elapsedMilliseconds}ms');
  }

  Future<SendResult> sendText(
    String channelId,
    int channelType,
    String text, {
    String? clientMsgNo,
  }) async {
    final r = await sdk.send(
      channelId: channelId,
      channelType: WuKongChannelType.fromValue(channelType),
      payload: {'type': 1, 'content': text},
      clientMsgNo: clientMsgNo,
    );
    log(
      '[$name] sent "$text" → reason=${r.reasonCode.value} seq=${r.messageSeq}',
    );
    return r;
  }

  void dispose() => sdk.dispose();
}

/// Easy SDK 不解码 RECV 的 payload：服务端发来的是 base64 字符串。
dynamic decodePayload(dynamic p) {
  if (p is String) {
    try {
      return jsonDecode(utf8.decode(base64Decode(p)));
    } catch (_) {
      return p;
    }
  }
  return p;
}

Future<List<Map<String, dynamic>>> channelHistory(
  String loginUid,
  String channelId,
  int channelType,
) async {
  final r = await http(
    'POST',
    '$wkApi/channel/messagesync',
    body: {
      'login_uid': loginUid,
      'channel_id': channelId,
      'channel_type': channelType,
      'start_message_seq': 0,
      'end_message_seq': 0,
      'limit': 100,
      'pull_mode': 1,
    },
  );
  final list = (r is Map ? r['messages'] : r) as List? ?? const [];
  return list.cast<Map<String, dynamic>>();
}

Future<void> sleep(int ms) => Future<void>.delayed(Duration(milliseconds: ms));

String arg(List<String> args, String name, String def) {
  final i = args.indexOf('--$name');
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}

Future<void> main(List<String> args) async {
  if (args.isEmpty) {
    stderr.writeln('用法见文件头注释');
    exit(64);
  }
  final scenario = args.first;
  final staffNo = int.parse(arg(args, 'staff', '3'));
  final flag = int.parse(arg(args, 'flag', '0'));
  final verbose = args.contains('--verbose');
  final done = Completer<int>();
  var unhandled = 0;
  // v1.2.1 连接失败时会留下一个没人监听的 Future 错误，纯 Dart 进程会因此退出；
  // 这里接住并计数，方便对比修复前后。
  runZonedGuarded(
    () async {
      try {
        switch (scenario) {
          case 'basic':
            await basic(staffNo, flag, verbose);
          case 'dupe':
            await dupe(staffNo, flag);
          case 'badtoken':
            await badToken(staffNo, flag);
          case 'refresh':
            await refresh(staffNo, flag);
          case 'kick':
            await kick(staffNo, flag);
          case 'connkick':
            await connKick(staffNo, flag);
          case 'coexist':
            await coexist(staffNo);
          case 'listen':
            await listen(staffNo, flag, int.parse(arg(args, 'seconds', '60')));
          case 'heartbeat':
            await heartbeat(staffNo, flag, verbose);
          case 'outage':
            await outage(staffNo, flag, int.parse(arg(args, 'seconds', '120')));
          case 'connloop':
            await connLoop(staffNo, flag, int.parse(arg(args, 'times', '50')));
          default:
            stderr.writeln('未知场景 $scenario');
            exit(64);
        }
      } catch (e, st) {
        log('FAILED: $e\n$st');
        done.complete(1);
        return;
      }
      done.complete(0);
    },
    (e, st) {
      unhandled++;
      log(
        'UNHANDLED 异步错误 #$unhandled（SDK 内部没人监听的 Future）: '
        '${e.runtimeType}: $e',
      );
    },
  );
  final code = await done.future;
  log('未处理的异步错误共 $unhandled 个');
  exit(code);
}

Future<(Staff, Peer)> connectStaff(
  int n,
  int flag, {
  String? url,
  bool verbose = false,
  String? deviceId,
}) async {
  final staff = await staffLogin(n);
  final token = await registerToken(staff.uid, flag);
  final peer = Peer('staff$n/flag$flag');
  await peer.init(
    uid: staff.uid,
    token: token,
    flag: flag,
    url: url,
    verbose: verbose,
    deviceId: deviceId,
  );
  await peer.connect();
  return (staff, peer);
}

Future<(Visitor, Peer)> connectVisitor({bool verbose = false}) async {
  final v = await visitorLogin();
  final peer = Peer('visitor');
  // 访客 SDK 用 Web(1) 设备类型、从属设备级别（后端注册）
  await peer.init(uid: v.uid, token: v.imToken, flag: 1, verbose: verbose);
  await peer.connect();
  return (v, peer);
}

Future<void> basic(int n, int flag, bool verbose) async {
  final (staff, sp) = await connectStaff(n, flag, verbose: verbose);
  final (v, vp) = await connectVisitor();
  log('staff uid=${staff.uid} visitor uid=${v.uid} channel=${v.channelId}');

  const v2s = '访客→客服：你好 Hello 🙂 <b>&amp;</b> "引号" \\n 换行\n第二行';
  await vp.sendText(v.channelId, 2, v2s);
  final gotV = await sp.waitFor(
    () => sp.messages.any((m) => decodePayload(m.payload)?['content'] == v2s),
    const Duration(seconds: 10),
  );
  log('客服收到访客消息且内容一致: $gotV');

  const s2v = '客服→访客：收到 👍 中文 émoji 🇨🇳';
  final r = await sp.sendText(v.channelId, 2, s2v);
  final gotS = await vp.waitFor(
    () => vp.messages.any((m) => decodePayload(m.payload)?['content'] == s2v),
    const Duration(seconds: 10),
  );
  log('访客收到客服消息且内容一致: $gotS (send reason=${r.reasonCode.value})');

  await sleep(1000);
  sp.dispose();
  vp.dispose();
  log('RESULT basic flag=$flag staff→visitor=$gotS visitor→staff=$gotV');
}

Future<void> dupe(int n, int flag) async {
  final (staff, sp) = await connectStaff(n, flag);
  final (v, vp) = await connectVisitor();
  final cmn = 'talk7-dupe-${DateTime.now().microsecondsSinceEpoch}';

  log('— 顺序发送：第一次收到 SENDACK 后，用同一个 clientMsgNo 再发一次');
  final r1 = await sp.sendText(v.channelId, 2, 'dupe A', clientMsgNo: cmn);
  final r2 = await sp.sendText(v.channelId, 2, 'dupe A', clientMsgNo: cmn);
  log(
    '两次 SENDACK: id1=${r1.messageId} seq1=${r1.messageSeq} '
    'id2=${r2.messageId} seq2=${r2.messageSeq}',
  );

  final cmn2 = '$cmn-b';
  log('— 并发发送：不等 SENDACK 连发两次');
  final rs = await Future.wait([
    sp.sendText(v.channelId, 2, 'dupe B', clientMsgNo: cmn2),
    sp.sendText(v.channelId, 2, 'dupe B', clientMsgNo: cmn2),
  ]);
  log('两次 SENDACK: seq=${rs.map((e) => e.messageSeq).toList()}');

  await sleep(3000);
  final pushA = vp.messages.where((m) => m.clientMsgNo == cmn).length;
  final pushB = vp.messages.where((m) => m.clientMsgNo == cmn2).length;
  final hist = await channelHistory(v.uid, v.channelId, 2);
  final storedA = hist.where((m) => m['client_msg_no'] == cmn).length;
  final storedB = hist.where((m) => m['client_msg_no'] == cmn2).length;
  sp.dispose();
  vp.dispose();
  log(
    'RESULT dupe 顺序: 访客实时收到 $pushA 条, 服务端存了 $storedA 条; '
    '并发: 访客实时收到 $pushB 条, 服务端存了 $storedB 条',
  );
}

Future<void> badToken(int n, int flag) async {
  final staff = await staffLogin(n);
  await registerToken(staff.uid, flag); // 确保设备存在，token 与下面的不同
  final p = Peer('badtoken');
  await p.init(uid: staff.uid, token: 'wrong-token', flag: flag);
  final sw = Stopwatch()..start();
  Object? err;
  try {
    await p.connect();
  } catch (e) {
    err = e;
  }
  log('connect() 结束 ${sw.elapsedMilliseconds}ms, 异常=${err.runtimeType}: $err');
  await sleep(8000); // 看之后有没有自动重连
  log('isConnected=${p.sdk.isConnected} 事件: ${p.events.length} 条');
  p.dispose();

  log('— 不存在的设备（从未注册过 token 的设备类型）');
  final q = Peer('nodevice');
  await q.init(
    uid: 'talk7-nobody-${DateTime.now().millisecondsSinceEpoch}',
    token: 'x',
    flag: flag,
  );
  sw.reset();
  try {
    await q.connect();
  } catch (e) {
    err = e;
  }
  log('connect() 结束 ${sw.elapsedMilliseconds}ms, 异常=${err.runtimeType}: $err');
  q.dispose();
  log('RESULT badtoken');
}

Future<void> refresh(int n, int flag) async {
  final (staff, sp) = await connectStaff(n, flag, deviceId: 'phone-A');
  await sleep(1000);
  log('— 用主设备级别为同一设备类型更新 token（App 自己刷新 token 时就是这样）');
  final sw = Stopwatch()..start();
  final newToken = await registerToken(staff.uid, flag);
  await sp.waitFor(
    () => sp.disconnects.isNotEmpty,
    const Duration(seconds: 15),
  );
  log('首个 disconnect 事件距更新 token ${sw.elapsedMilliseconds}ms');
  await sleep(40000); // 观察 SDK 自动重连（旧 token）结果
  log('40 秒后 isConnected=${sp.sdk.isConnected}');
  sp.dispose();

  log('— 刷新后用新 token 重新 init + connect');
  final again = Peer('staff$n/flag$flag-new');
  await again.init(
    uid: staff.uid,
    token: newToken,
    flag: flag,
    deviceId: 'phone-A',
  );
  await again.connect();
  await sleep(3000);
  log(
    '新 token 连接 isConnected=${again.sdk.isConnected} '
    'disconnects=${again.disconnects.length}',
  );
  again.dispose();
  log('RESULT refresh');
}

Future<void> kick(int n, int flag) async {
  final (staff, a) = await connectStaff(n, flag, deviceId: 'phone-A');
  await sleep(1000);
  log('— 另一台同类型设备登录：后端为它更新 token（主设备级别），它用新 deviceId 连上');
  final sw = Stopwatch()..start();
  final tokenB = await registerToken(staff.uid, flag);
  final b = Peer('phone-B');
  await b.init(uid: staff.uid, token: tokenB, flag: flag, deviceId: 'phone-B');
  await b.connect();
  await a.waitFor(() => a.disconnects.isNotEmpty, const Duration(seconds: 15));
  log('A 首个 disconnect 距 B 登录 ${sw.elapsedMilliseconds}ms');
  await sleep(40000);
  log(
    '40 秒后 A isConnected=${a.sdk.isConnected}, B isConnected=${b.sdk.isConnected} '
    'B disconnects=${b.disconnects.length}',
  );
  a.dispose();
  b.dispose();
  log('RESULT kick');
}

Future<void> connKick(int n, int flag) async {
  final staff = await staffLogin(n);
  final token = await registerToken(staff.uid, flag);
  final a = Peer('conn-A');
  await a.init(uid: staff.uid, token: token, flag: flag, deviceId: 'dev-A');
  await a.connect();
  await sleep(1000);
  final b = Peer('conn-B');
  await b.init(uid: staff.uid, token: token, flag: flag, deviceId: 'dev-B');
  await b.connect();
  await a.waitFor(() => a.disconnects.isNotEmpty, const Duration(seconds: 15));
  await sleep(15000);
  log(
    'A isConnected=${a.sdk.isConnected} B isConnected=${b.sdk.isConnected} '
    'B disconnects=${b.disconnects.length}',
  );
  a.dispose();
  b.dispose();
  log('RESULT connkick');
}

Future<void> coexist(int n) async {
  final staff = await staffLogin(n);
  final web = Peer('web(1)');
  // 连另一个 WuKongIM 时没有业务登录，直接按后端的方式注册 Web(1) 主设备 token
  Future<String> webToken(Staff s) async =>
      s.webImToken.isNotEmpty ? s.webImToken : registerToken(s.uid, 1);
  await web.init(
    uid: staff.uid,
    token: await webToken(staff),
    flag: 1,
    deviceId: 'browser',
  );
  await web.connect();
  final appToken = await registerToken(staff.uid, 0);
  final app = Peer('app(0)');
  await app.init(uid: staff.uid, token: appToken, flag: 0, deviceId: 'phone');
  await app.connect();
  final pcToken = await registerToken(staff.uid, 2);
  final pc = Peer('desktop(2)');
  await pc.init(uid: staff.uid, token: pcToken, flag: 2, deviceId: 'mac');
  await pc.connect();
  log('三端都已连上，保持 30 秒');
  await sleep(30000);
  log('— 网页版重新登录（后端以 Web 主设备级别更新 token）');
  final relogin = await staffLogin(n);
  final web2 = Peer('web(1)-relogin');
  await web2.init(
    uid: staff.uid,
    token: await webToken(relogin),
    flag: 1,
    deviceId: 'browser2',
  );
  await web2.connect();
  await sleep(15000);
  log(
    'RESULT coexist: web 旧连接 disconnects=${web.disconnects.length}, '
    'app disconnects=${app.disconnects.length} connected=${app.sdk.isConnected}, '
    'desktop disconnects=${pc.disconnects.length} connected=${pc.sdk.isConnected}, '
    'web 新连接 connected=${web2.sdk.isConnected}',
  );
  for (final p in [web, app, pc, web2]) {
    p.dispose();
  }
}

Future<void> listen(int n, int flag, int seconds) async {
  final (staff, sp) = await connectStaff(n, flag);
  // 同时建一个访客并发一条消息，让它出现在待接入里；uid 写到 TALK7_VISITOR_FILE
  final (v, vp) = await connectVisitor();
  await vp.sendText(v.channelId, 2, 'listen 场景的访客消息');
  final f = Platform.environment['TALK7_VISITOR_FILE'];
  if (f != null) File(f).writeAsStringSync(v.uid);
  log('staff uid=${staff.uid} visitor uid=${v.uid}，监听 $seconds 秒');
  await sleep(seconds * 1000);
  Map<int, int> byType(Peer p) {
    final types = <int, int>{};
    for (final m in p.messages) {
      final d = decodePayload(m.payload);
      final t = d is Map ? (d['type'] as num?)?.toInt() ?? -1 : -1;
      types[t] = (types[t] ?? 0) + 1;
    }
    return types;
  }

  sp.dispose();
  vp.dispose();
  log(
    'RESULT listen 客服共 ${sp.messages.length} 条，按类型: ${byType(sp)}；'
    '访客共 ${vp.messages.length} 条，按类型: ${byType(vp)}',
  );
}

Future<void> heartbeat(int n, int flag, bool verbose) async {
  final (_, sp) = await connectStaff(n, flag, url: wsProxied, verbose: verbose);
  await sleep(1000);
  log('— netproxy /drop：已建立的连接不再收发，新连接收不到回应');
  await http('GET', '$netproxy/drop?ports=25002');
  final sw = Stopwatch()..start();
  // 心跳 25s 一次，pong 超时 10s：最多约 35s 应该发现连接已死
  final detected = await sp.waitFor(
    () => sp.errors.any((e) => e.message.contains('Ping timeout')),
    const Duration(seconds: 60),
  );
  log('心跳超时检测: $detected (${sw.elapsedMilliseconds}ms)');
  await sleep(5000);
  log('— netproxy /up');
  await http('GET', '$netproxy/up');
  final before = sp.connects;
  final back = await sp.waitFor(
    () => sp.connects > before,
    const Duration(seconds: 60),
  );
  log(
    '/up 之后自动连回: $back, isConnected=${sp.sdk.isConnected} '
    '(距 drop ${sw.elapsedMilliseconds}ms)',
  );
  sp.dispose();
  log('RESULT heartbeat detected=$detected reconnected=$back');
}

Future<void> connLoop(int n, int flag, int times) async {
  final staff = await staffLogin(n);
  final token = await registerToken(staff.uid, flag);
  final failures = <String, int>{};
  var zero = 0;
  for (var i = 0; i < times; i++) {
    final p = Peer('loop$i');
    await p.init(uid: staff.uid, token: token, flag: flag, deviceId: 'loop');
    try {
      await p.connect();
    } catch (e) {
      final k = e.toString().split('\n').first;
      failures[k] = (failures[k] ?? 0) + 1;
    }
    if (p.events.any((e) => e.contains('timeDiff: 0,'))) zero++;
    p.dispose();
    await sleep(50);
  }
  log(
    'RESULT connloop $times 次: 失败 ${failures.values.fold(0, (a, b) => a + b)} 次 $failures; '
    '成功且 timeDiff=0 的 $zero 次',
  );
}

Future<void> outage(int n, int flag, int seconds) async {
  final (_, sp) = await connectStaff(n, flag, url: wsProxied);
  await sleep(1000);
  log('— netproxy /drop，持续 $seconds 秒');
  await http('GET', '$netproxy/drop?ports=25002');
  await sleep(seconds * 1000);
  log('— netproxy /up');
  await http('GET', '$netproxy/up');
  final before = sp.connects;
  final back = await sp.waitFor(
    () => sp.connects > before,
    const Duration(seconds: 30),
  );
  sp.dispose();
  log('RESULT outage ${seconds}s: /up 后 30 秒内自动连回=$back');
}
