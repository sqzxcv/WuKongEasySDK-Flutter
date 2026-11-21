import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:web_socket_channel/web_socket_channel.dart';

import '../enums/wukong_event.dart';
import '../enums/wukong_channel_type.dart';
import '../exceptions/wukong_exceptions.dart';
import '../models/connect_result.dart';
import '../models/disconnect_info.dart';
import '../models/event_notification.dart';
import '../models/message.dart';
import '../models/reconnecting_info.dart';
import '../models/send_result.dart';
import '../models/message_setting.dart';
import '../models/wukong_error.dart';
import '../utils/json_rpc.dart';
import '../utils/uuid_generator.dart';
import '../utils/wukong_constants.dart';
import '../utils/wukong_logger.dart';
import 'event_manager.dart';
import 'wukong_config.dart';

/// WebSocket client for WuKong communication
///
/// Handles WebSocket connection, JSON-RPC protocol, ping/pong heartbeat, and reconnection logic.
class WuKongClient {
  static const Set<String> _knownProtocolMethods = {
    'connect',
    'send',
    'recv',
    'recvack',
    'ping',
    'pong',
    'disconnect',
    'event',
    'sendack',
  };

  /// WebSocket channel
  WebSocketChannel? _channel;

  /// Configuration
  final WuKongConfig _config;

  /// Event manager
  final EventManager _eventManager;

  /// Configuration-controlled diagnostic logger.
  final WuKongLogger _logger;

  /// Whether the client is connected
  bool _isConnected = false;

  /// Whether the client is connecting
  bool _isConnecting = false;

  /// Whether manual disconnect was initiated
  bool _manualDisconnect = false;

  /// Whether reconnection is in progress
  bool _isReconnecting = false;

  /// Current reconnection attempt count
  int _reconnectAttempts = 0;

  /// Pending requests waiting for responses
  final Map<String, PendingRequest> _pendingRequests = {};

  /// Ping timer
  Timer? _pingTimer;

  /// Connection completer
  Completer<void>? _connectionCompleter;

  /// Stream subscription for WebSocket messages
  StreamSubscription? _messageSubscription;

  /// Interval between heartbeat pings.
  final Duration _pingInterval;

  /// How long to wait for a pong before treating the connection as dead.
  final Duration _pongTimeout;

  /// Delay before the first reconnect attempt; doubles on each failure.
  final Duration _initialReconnectDelay;

  /// Pending automatic reconnect attempt.
  Timer? _reconnectTimer;

  /// Creates a new WuKong client.
  ///
  /// The timing parameters default to [WuKongConstants]; tests shorten them.
  WuKongClient(
    this._config,
    this._eventManager,
    this._logger, {
    Duration? pingInterval,
    Duration? pongTimeout,
    Duration? initialReconnectDelay,
  })  : _pingInterval = pingInterval ??
            const Duration(milliseconds: WuKongConstants.pingIntervalMs),
        _pongTimeout = pongTimeout ??
            const Duration(milliseconds: WuKongConstants.pongTimeoutMs),
        _initialReconnectDelay = initialReconnectDelay ??
            const Duration(
                milliseconds: WuKongConstants.initialReconnectDelayMs);

  /// Whether the client is connected
  bool get isConnected => _isConnected;

  /// Whether the client is connecting
  bool get isConnecting => _isConnecting;

  /// Connects to the WebSocket server.
  ///
  /// An explicit call replaces any pending automatic reconnect: if it fails,
  /// the caller decides whether to try again.
  Future<void> connect() {
    _cancelReconnect();
    return _connect();
  }

  Future<void> _connect() async {
    if (_isConnected || _isConnecting) {
      _logger.log('Already connected or connecting');
      if (_connectionCompleter != null) {
        return _connectionCompleter!.future;
      }
      return;
    }

    _manualDisconnect = false;
    _isConnecting = true;
    _connectionCompleter = Completer<void>();
    final future = _connectionCompleter!.future;
    // On failure this method rethrows instead of returning [future], so the
    // completer's error would otherwise be reported as an uncaught async
    // error. Concurrent callers that joined via [_connectionCompleter] still
    // receive it.
    future.ignore();

    try {
      _logger.log('Connecting to WuKong server...');

      // Create WebSocket connection
      _channel = WebSocketChannel.connect(Uri.parse(_config.serverUrl));
      // A failed handshake is also delivered to the stream listener below,
      // which reports it and closes the attempt; without a listener the
      // `ready` future's copy of the error would escape as uncaught.
      _channel!.ready.ignore();

      // Listen for messages
      _messageSubscription = _channel!.stream.listen(
        _handleMessage,
        onError: _handleError,
        onDone: _handleDisconnect,
      );

      // Send authentication request
      await _sendConnectRequest();

      if (_connectionCompleter != null && !_connectionCompleter!.isCompleted) {
        _connectionCompleter!.complete();
      }
    } catch (error) {
      _logger.log('Connection failed: ${error.runtimeType}');
      _isConnecting = false;
      if (_connectionCompleter != null && !_connectionCompleter!.isCompleted) {
        _connectionCompleter!.completeError(error);
      }
      _cleanup();
      rethrow;
    } finally {
      _connectionCompleter = null;
    }

    return future;
  }

  /// Disconnects from the WebSocket server
  void disconnect() {
    _logger.log('Manual disconnect initiated');
    _manualDisconnect = true;
    _cancelReconnect();
    _handleDisconnect();
  }

  /// Sends a message
  Future<SendResult> send({
    required String channelId,
    required WuKongChannelType channelType,
    required dynamic payload,
    String? clientMsgNo,
    Map<String, dynamic>? header,
    String? topic,
    dynamic setting,
  }) async {
    if (!_isConnected) {
      throw const WuKongNotConnectedException();
    }

    if (payload == null) {
      throw const WuKongConfigurationException('Payload cannot be null');
    }

    // Set default header values
    final messageHeader = <String, dynamic>{
      'redDot': true,
      ...?header,
    };

    // Encode payload to base64: server expects []uint8 (base64 string in JSON)
    final payloadBytes = utf8.encode(jsonEncode(payload));
    final payloadBase64 = base64Encode(payloadBytes);

    final params = {
      'clientMsgNo': clientMsgNo ?? UuidGenerator.generate(),
      'channelId': channelId,
      'channelType': channelType.value,
      'payload': payloadBase64,
      'header': messageHeader,
      if (topic != null) 'topic': topic,
      if (setting != null)
        'setting': setting is MessageSetting ? setting.toJson() : setting,
    };

    final response = await _sendRequest('send', params);
    return SendResult.fromJson(response);
  }

  /// Sends a JSON-RPC request and waits for response
  Future<dynamic> _sendRequest(String method, dynamic params,
      [int? timeoutMs]) async {
    if (_channel == null) {
      throw const WuKongNotConnectedException();
    }

    final requestId = UuidGenerator.generate();
    final request = JsonRpcRequest(
      method: method,
      params: params,
      id: requestId,
    );

    final completer = Completer<dynamic>();
    final timeout = timeoutMs ?? WuKongConstants.defaultRequestTimeoutMs;

    // Set up timeout
    final timer = Timer(Duration(milliseconds: timeout), () {
      _pendingRequests.remove(requestId);
      if (!completer.isCompleted) {
        completer.completeError(
          WuKongConnectionTimeoutException(
              'Request timeout for method $method'),
        );
      }
    });

    _pendingRequests[requestId] = PendingRequest(
      resolve: (result) {
        timer.cancel();
        if (!completer.isCompleted) {
          completer.complete(result);
        }
      },
      reject: (error) {
        timer.cancel();
        if (!completer.isCompleted) {
          completer.completeError(error);
        }
      },
      timeoutTimer: timer,
    );

    try {
      final message = jsonEncode(request.toJson());
      _logger.log(
        '--> JSON-RPC request method=${_diagnosticMethod(method)} id=present',
      );
      _channel!.sink.add(message);
    } catch (error) {
      timer.cancel();
      _pendingRequests.remove(requestId);
      rethrow;
    }

    return completer.future;
  }

  /// Sends a JSON-RPC notification (no response expected)
  void _sendNotification(String method, dynamic params) {
    if (_channel == null) {
      _logger.log('Cannot send notification, not connected');
      return;
    }

    final notification = JsonRpcNotification(
      method: method,
      params: params,
    );

    try {
      final message = jsonEncode(notification.toJson());
      _logger.log(
        '--> JSON-RPC notification method=${_diagnosticMethod(method)}',
      );
      _channel!.sink.add(message);
    } catch (error) {
      _logger.log('JSON-RPC notification failed: ${error.runtimeType}');
    }
  }

  /// Sends the connect/authentication request
  Future<void> _sendConnectRequest() async {
    try {
      final params = _config.toAuthParams();
      final result = await _sendRequest(
          'connect', params, WuKongConstants.connectTimeoutMs);

      final connectResult = ConnectResult.fromJson(result);
      _logger.log('Authentication successful');

      _isConnected = true;
      _isConnecting = false;
      _reconnectAttempts = 0;
      _isReconnecting = false;

      _startPing();
      _eventManager.emit(WuKongEvent.connect, connectResult);
    } catch (error) {
      _logger.log('Authentication failed: ${error.runtimeType}');
      _isConnecting = false;

      final wukongError = error is WuKongException
          ? WuKongError.authError(error.message)
          : WuKongError.authError('Authentication failed: $error');

      _eventManager.emit(WuKongEvent.error, wukongError);
      _cleanup();
      rethrow;
    }
  }

  /// Handles incoming WebSocket messages
  void _handleMessage(dynamic data) {
    try {
      _logger.log('<-- WebSocket message received');
      final json = jsonDecode(data.toString()) as Map<String, dynamic>;

      if (json.containsKey('id')) {
        // It's a response
        _handleResponse(JsonRpcResponse.fromJson(json));
      } else if (json.containsKey('method')) {
        // It's a notification
        _handleNotification(JsonRpcNotification.fromJson(json));
      } else {
        _logger.log('Unknown JSON-RPC message shape');
      }
    } catch (error) {
      _logger.log('JSON-RPC message parsing failed: ${error.runtimeType}');
      _eventManager.emit(
          WuKongEvent.error,
          WuKongError.fromException(
            Exception('Failed to parse JSON-RPC message'),
          ));
    }
  }

  /// Handles JSON-RPC responses
  void _handleResponse(JsonRpcResponse response) {
    _logger.log('<-- Handling response id=present');

    final pending = _pendingRequests.remove(response.id);
    if (pending != null) {
      if (response.isError) {
        final error =
            WuKongProtocolException('Server error: ${response.error!.message}');
        pending.reject(error);
      } else {
        pending.resolve(response.result);
      }
    } else {
      _logger.log('Received response for unknown request id=present');
    }
  }

  /// Handles JSON-RPC notifications
  void _handleNotification(JsonRpcNotification notification) {
    final diagnosticMethod = _diagnosticMethod(notification.method);
    _logger.log('<-- Handling notification method=$diagnosticMethod');

    switch (notification.method) {
      case 'recv':
        _handleRecvNotification(notification.params);
        break;
      case 'pong':
        _handlePongNotification();
        break;
      case 'disconnect':
        _handleServerDisconnect(notification.params);
        break;
      case 'event':
        _handleEventNotification(notification.params);
        break;
      case 'sendack':
        _handleSendAckNotification(notification.params);
        break;
      default:
        _logger.log('Unknown notification method=unknown');
    }
  }

  /// Handles incoming message notifications
  void _handleRecvNotification(dynamic params) {
    try {
      final message = Message.fromJson(params as Map<String, dynamic>);
      _eventManager.emit(WuKongEvent.message, message);

      // Send acknowledgment
      _sendRecvAck(
          message.header.toJson(), message.messageId, message.messageSeq);
    } catch (error) {
      _logger.log('RECV notification handling failed: ${error.runtimeType}');
    }
  }

  /// Handles pong notifications
  void _handlePongNotification() {
    _logger.log('Pong received');
  }

  /// Handles server-initiated disconnect
  ///
  /// WuKongIM sends DISCONNECT when it kicks the session: the token was
  /// replaced (reason "账号在其他设备上登录"), another device of the same type
  /// logged in ("login in other device"), or /user/device_quit was called (no
  /// reason). All use reasonCode 12 (connectKick). Reconnecting with the same
  /// credentials either fails authentication or kicks the other device in
  /// turn, so this is terminal: stop automatic reconnection and report a
  /// single disconnect carrying the server's code and reason. The app decides
  /// whether to call [connect] again, e.g. with a refreshed token.
  void _handleServerDisconnect(dynamic params) {
    _logger.log('Server initiated disconnect');
    final disconnectInfo =
        DisconnectInfo.fromJson(params as Map<String, dynamic>? ?? {});
    _manualDisconnect = true;
    _cancelReconnect();
    _cleanup();
    _eventManager.emit(WuKongEvent.disconnect, disconnectInfo);
  }

  /// Handles custom event notifications
  void _handleEventNotification(dynamic params) {
    try {
      final event = EventNotification.fromJson(params as Map<String, dynamic>);
      _eventManager.emit(WuKongEvent.customEvent, event);
    } catch (error) {
      _logger.log('Event notification handling failed: ${error.runtimeType}');
    }
  }

  /// Handles send acknowledgment notifications
  void _handleSendAckNotification(dynamic params) {
    try {
      final result = SendResult.fromJson(params as Map<String, dynamic>);
      _eventManager.emit(WuKongEvent.sendAck, result);
    } catch (error) {
      _logger.log('SENDACK notification handling failed: ${error.runtimeType}');
    }
  }

  /// Sends message acknowledgment
  void _sendRecvAck(
      Map<String, dynamic> header, String messageId, int messageSeq) {
    final params = {
      'header': header,
      'messageId': messageId,
      'messageSeq': messageSeq,
    };
    _sendNotification('recvack', params);
  }

  /// Starts the ping timer
  void _startPing() {
    _stopPing();

    _pingTimer = Timer.periodic(
      _pingInterval,
      (timer) {
        if (_isConnected && _channel != null) {
          _sendPing();
        } else {
          _stopPing();
        }
      },
    );

    _logger
        .log('Ping timer started (${_pingInterval.inMilliseconds}ms interval)');
  }

  /// Stops the ping timer
  void _stopPing() {
    _pingTimer?.cancel();
    _pingTimer = null;
    _logger.log('Ping timer stopped');
  }

  /// Sends a ping request
  void _sendPing() {
    final channel = _channel;
    _sendRequest('ping', {}, _pongTimeout.inMilliseconds).catchError((error) {
      // Pings rejected because the socket already closed are handled by
      // _handleDisconnect. Only a timeout on the current socket means the
      // connection is half-open.
      if (error is! WuKongConnectionTimeoutException ||
          !identical(channel, _channel) ||
          !_isConnected) {
        return null;
      }
      _logger.log('Ping timeout, dropping the connection');
      _eventManager.emit(
          WuKongEvent.error, WuKongError.timeoutError('Ping timeout'));
      // A half-open socket may never report onDone, so close it through the
      // normal disconnect path, which schedules a reconnect. Calling
      // _tryReconnect() alone left _isConnected set: the scheduled connect()
      // returned early, _isReconnecting stayed true and no later disconnect
      // could start reconnecting again.
      _handleDisconnect();
      return null;
    });
  }

  /// Handles WebSocket errors
  void _handleError(dynamic error) {
    _logger.log('WebSocket failed: ${error.runtimeType}');

    final wukongError = WuKongError.networkError('WebSocket error: $error');
    _eventManager.emit(WuKongEvent.error, wukongError);
  }

  /// Handles WebSocket disconnection
  void _handleDisconnect() {
    final wasConnected = _isConnected;
    _logger.log('WebSocket disconnected. Was connected: $wasConnected');

    final disconnectInfo = DisconnectInfo(
      code: _manualDisconnect ? 1000 : 1006,
      reason: _manualDisconnect ? 'Manual disconnect' : 'Connection lost',
    );

    _cleanup();
    _eventManager.emit(WuKongEvent.disconnect, disconnectInfo);

    // Try to reconnect if it wasn't a manual disconnect and we were previously connected
    if (!_manualDisconnect && wasConnected) {
      _tryReconnect();
    }
  }

  /// Attempts to reconnect with exponential backoff
  void _tryReconnect() {
    if (_isReconnecting || _manualDisconnect) {
      return;
    }

    _isReconnecting = true;
    _scheduleReconnect();
  }

  /// Schedules the next reconnection attempt
  void _scheduleReconnect() {
    if (_reconnectAttempts >= WuKongConstants.maxReconnectAttempts) {
      _logger.log('Max reconnect attempts reached. Giving up.');
      _isReconnecting = false;
      _reconnectAttempts = 0;
      _eventManager.emit(
          WuKongEvent.error, WuKongError.networkError('Reconnection failed'));
      return;
    }

    final delay =
        _initialReconnectDelay.inMilliseconds * pow(2, _reconnectAttempts);
    _reconnectAttempts++;

    _logger
        .log('Scheduling reconnect attempt $_reconnectAttempts in ${delay}ms');

    _eventManager.emit(
      WuKongEvent.reconnecting,
      ReconnectingInfo(
        attempt: _reconnectAttempts,
        delay: delay.toInt(),
      ),
    );

    _reconnectTimer?.cancel();
    _reconnectTimer = Timer(Duration(milliseconds: delay.toInt()), () {
      _reconnectTimer = null;
      if (!_isReconnecting) {
        _logger.log('Reconnection cancelled');
        return;
      }

      _connect().catchError((error) {
        _logger.log('Reconnection attempt failed: ${error.runtimeType}');
        if (_isReconnecting) {
          _scheduleReconnect();
        }
      });
    });
  }

  /// Stops automatic reconnection and resets its backoff.
  void _cancelReconnect() {
    _isReconnecting = false;
    _reconnectAttempts = 0;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  /// Cleans up resources
  void _cleanup() {
    _isConnected = false;
    _isConnecting = false;
    _stopPing();

    // Cancel all pending requests
    for (final pending in _pendingRequests.values) {
      if (pending.timeoutTimer is Timer) {
        (pending.timeoutTimer as Timer).cancel();
      }
      pending.reject(const WuKongNetworkException('Connection closed'));
    }
    _pendingRequests.clear();

    // Close WebSocket
    _messageSubscription?.cancel();
    _messageSubscription = null;

    try {
      _channel?.sink.close();
    } catch (error) {
      _logger.log('WebSocket close failed: ${error.runtimeType}');
    }
    _channel = null;

    // Complete connection completer with error if still pending
    if (_connectionCompleter != null && !_connectionCompleter!.isCompleted) {
      _connectionCompleter!
          .completeError(const WuKongNetworkException('Connection closed'));
      _connectionCompleter = null;
    }
  }

  /// Disposes the client and cleans up all resources
  void dispose() {
    _logger.log('Disposing WuKong client');
    _manualDisconnect = true;
    _cancelReconnect();
    _cleanup();
  }

  /// Returns only protocol method names controlled by this SDK.
  static String _diagnosticMethod(String method) {
    return _knownProtocolMethods.contains(method) ? method : 'unknown';
  }
}
