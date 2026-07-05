library open_event_flow;

import 'dart:async';
import 'dart:math';

class OpenEventFlowClient {
  OpenEventFlowClient({
    required this.app,
    required this.transport,
    EventStore? store,
    String? anonymousId,
    DateTime Function()? clock,
    String Function()? idGenerator,
    int stayMinDurationMs = 1000,
    int stayMaxDurationMs = 24 * 60 * 60 * 1000,
  })  : _store = store ?? MemoryEventStore(),
        _clock = clock ?? DateTime.now,
        _idGenerator = idGenerator ?? _defaultId,
        _identity = UserIdentity(anonymousId: anonymousId ?? _defaultId()),
        _stayMinDurationMs = stayMinDurationMs,
        _stayMaxDurationMs = stayMaxDurationMs;

  final OpenEventFlowAppContext app;
  final OpenEventFlowTransport transport;
  final EventStore _store;
  final DateTime Function() _clock;
  final String Function() _idGenerator;
  final UserIdentity _identity;
  final int _stayMinDurationMs;
  final int _stayMaxDurationMs;
  final Map<String, StayState> _stays = {};
  AnalyticsConsent _consent = const AnalyticsConsent(analyticsAllowed: true);
  late SessionContext _session = SessionContext.start(_idGenerator, _clock);

  Future<TrackResult> track(AnalyticsEvent event) async {
    if (!_consent.analyticsAllowed) {
      return const TrackResult(accepted: false, reason: 'analytics_consent_disabled');
    }
    final normalized = NormalizedEvent(
      eventId: _idGenerator(),
      eventName: event.name,
      schema: event.schema,
      clientTime: _clock(),
      properties: _sanitize(event.properties),
      context: EventContext(
        app: app,
        user: _identity,
        session: _session,
        consent: _consent,
      ),
    );
    await _store.push(normalized);
    return TrackResult(accepted: true, eventId: normalized.eventId);
  }

  Future<TrackResult> screen(String name, {Map<String, Object?> properties = const {}}) {
    return track(
      SimpleAnalyticsEvent(
        name: 'screen_view',
        schema: 'iglu:io.openeventflow/screen_view/jsonschema/1-0-0',
        properties: {'name': name, ...properties},
      ),
    );
  }

  void identify(String userId, {Map<String, Object?> traits = const {}}) {
    _identity.userId = userId;
    _identity.traits = Map.unmodifiable(traits);
  }

  void setConsent(AnalyticsConsent consent) {
    _consent = consent;
  }

  String startNewSession() {
    _session = SessionContext.start(_idGenerator, _clock);
    return _session.sessionId;
  }

  Future<FlushResult> flush({int batchSize = 50}) async {
    final batch = await _store.peek(batchSize);
    if (batch.isEmpty) {
      return const FlushResult(sent: 0, remaining: 0);
    }
    await transport.send(batch);
    await _store.remove(batch.length);
    return FlushResult(sent: batch.length, remaining: await _store.size());
  }

  Future<int> queueSize() => _store.size();

  StayStartResult beginStay(String key, {Map<String, Object?> properties = const {}}) {
    if (key.isEmpty) {
      throw ArgumentError('stay key is required');
    }
    final startedAt = _clock();
    final stay = StayState(
      key: key,
      stayId: _idGenerator(),
      properties: Map.unmodifiable(properties),
      startedAt: startedAt,
      activeStartedAt: startedAt,
    );
    _stays[key] = stay;
    return StayStartResult(accepted: true, stayId: stay.stayId, startedAt: startedAt);
  }

  StayUpdateResult pauseStay(String key) {
    final stay = _stays[key];
    if (stay == null) {
      return const StayUpdateResult(accepted: false, reason: 'stay_not_found');
    }
    if (stay.paused) {
      return StayUpdateResult(accepted: true, durationMs: _currentStayDuration(stay, _clock()));
    }
    final now = _clock();
    stay.accumulatedMs += now.difference(stay.activeStartedAt).inMilliseconds;
    stay.paused = true;
    stay.pausedAt = now;
    return StayUpdateResult(accepted: true, durationMs: _clampStayDuration(stay.accumulatedMs));
  }

  StayUpdateResult resumeStay(String key) {
    final stay = _stays[key];
    if (stay == null) {
      return const StayUpdateResult(accepted: false, reason: 'stay_not_found');
    }
    if (!stay.paused) {
      return StayUpdateResult(accepted: true, durationMs: _currentStayDuration(stay, _clock()));
    }
    stay.activeStartedAt = _clock();
    stay.paused = false;
    stay.pausedAt = null;
    return StayUpdateResult(accepted: true, durationMs: _clampStayDuration(stay.accumulatedMs));
  }

  Future<StayEndResult> endStay(
    String key, {
    String exitReason = 'unknown',
    Map<String, Object?> properties = const {},
  }) async {
    final stay = _stays.remove(key);
    if (stay == null) {
      return const StayEndResult(accepted: false, reason: 'stay_not_found');
    }
    final durationMs = _currentStayDuration(stay, _clock());
    if (durationMs < _stayMinDurationMs) {
      return StayEndResult(
        accepted: false,
        durationMs: durationMs,
        stayId: stay.stayId,
        reason: 'stay_duration_below_minimum',
      );
    }
    if (!_consent.analyticsAllowed) {
      return StayEndResult(
        accepted: false,
        durationMs: durationMs,
        stayId: stay.stayId,
        reason: 'analytics_consent_disabled',
      );
    }
    final result = await track(
      SimpleAnalyticsEvent(
        name: 'page_stay',
        schema: 'iglu:io.openeventflow/page_stay/jsonschema/1-0-0',
        properties: {
          ...stay.properties,
          ...properties,
          'stay_id': stay.stayId,
          'duration_ms': durationMs,
          'exit_reason': exitReason,
        },
      ),
    );
    return StayEndResult(
      accepted: result.accepted,
      durationMs: durationMs,
      stayId: stay.stayId,
      reason: result.reason,
    );
  }

  Future<StayEndResult> switchStay(
    String key, {
    Map<String, Object?> properties = const {},
    String exitReason = 'route_change',
  }) async {
    final ended = _stays.containsKey(key)
        ? await endStay(key, exitReason: exitReason)
        : const StayEndResult(accepted: false, reason: 'stay_not_found');
    beginStay(key, properties: properties);
    return ended;
  }

  StayUpdateResult cancelStay(String key) {
    return StayUpdateResult(accepted: _stays.remove(key) != null);
  }

  Future<List<StayEndResult>> flushActiveStays({String exitReason = 'app_shutdown'}) async {
    final keys = List<String>.from(_stays.keys);
    final results = <StayEndResult>[];
    for (final key in keys) {
      results.add(await endStay(key, exitReason: exitReason));
    }
    return results;
  }

  Map<String, Object?> _sanitize(Map<String, Object?> properties) {
    if (_consent.piiAllowed) {
      return Map.unmodifiable(properties);
    }
    return Map.unmodifiable(
      Map.fromEntries(
        properties.entries.where((entry) => !_looksLikePii(entry.key)),
      ),
    );
  }

  int _currentStayDuration(StayState stay, DateTime now) {
    final activeDuration = stay.paused ? 0 : now.difference(stay.activeStartedAt).inMilliseconds;
    return _clampStayDuration(stay.accumulatedMs + activeDuration);
  }

  int _clampStayDuration(int durationMs) {
    return max(0, min(durationMs, _stayMaxDurationMs));
  }
}

abstract interface class AnalyticsEvent {
  String get name;
  String get schema;
  Map<String, Object?> get properties;
}

class SimpleAnalyticsEvent implements AnalyticsEvent {
  const SimpleAnalyticsEvent({
    required this.name,
    required this.schema,
    required this.properties,
  });

  @override
  final String name;

  @override
  final String schema;

  @override
  final Map<String, Object?> properties;
}

abstract interface class OpenEventFlowTransport {
  Future<void> send(List<NormalizedEvent> batch);
}

class CallbackTransport implements OpenEventFlowTransport {
  const CallbackTransport(this.callback);

  final FutureOr<void> Function(List<NormalizedEvent> batch) callback;

  @override
  Future<void> send(List<NormalizedEvent> batch) async {
    await callback(batch);
  }
}

abstract interface class EventStore {
  Future<void> push(NormalizedEvent event);
  Future<List<NormalizedEvent>> peek(int limit);
  Future<void> remove(int count);
  Future<int> size();
}

class MemoryEventStore implements EventStore {
  final List<NormalizedEvent> _events = [];

  @override
  Future<void> push(NormalizedEvent event) async {
    _events.add(event);
  }

  @override
  Future<List<NormalizedEvent>> peek(int limit) async {
    return List.unmodifiable(_events.take(limit));
  }

  @override
  Future<void> remove(int count) async {
    _events.removeRange(0, min(count, _events.length));
  }

  @override
  Future<int> size() async => _events.length;
}

class OpenEventFlowAppContext {
  const OpenEventFlowAppContext({
    required this.appId,
    required this.platform,
    required this.appVersion,
    required this.sdkVersion,
  });

  final String appId;
  final String platform;
  final String appVersion;
  final String sdkVersion;
}

class UserIdentity {
  UserIdentity({
    required this.anonymousId,
    this.userId,
    this.traits = const {},
  });

  final String anonymousId;
  String? userId;
  Map<String, Object?> traits;
}

class SessionContext {
  const SessionContext({
    required this.sessionId,
    required this.startedAt,
  });

  factory SessionContext.start(
    String Function() idGenerator,
    DateTime Function() clock,
  ) {
    return SessionContext(sessionId: idGenerator(), startedAt: clock());
  }

  final String sessionId;
  final DateTime startedAt;
}

class AnalyticsConsent {
  const AnalyticsConsent({
    required this.analyticsAllowed,
    this.advertisingAllowed = false,
    this.piiAllowed = false,
  });

  final bool analyticsAllowed;
  final bool advertisingAllowed;
  final bool piiAllowed;
}

class EventContext {
  const EventContext({
    required this.app,
    required this.user,
    required this.session,
    required this.consent,
  });

  final OpenEventFlowAppContext app;
  final UserIdentity user;
  final SessionContext session;
  final AnalyticsConsent consent;
}

class NormalizedEvent {
  const NormalizedEvent({
    required this.eventId,
    required this.eventName,
    required this.schema,
    required this.clientTime,
    required this.properties,
    required this.context,
  });

  final String eventId;
  final String eventName;
  final String schema;
  final DateTime clientTime;
  final Map<String, Object?> properties;
  final EventContext context;

  Map<String, Object?> toJson() {
    return {
      'event_id': eventId,
      'event_name': eventName,
      'schema': schema,
      'client_time': clientTime.toIso8601String(),
      'properties': properties,
      'context': {
        'app': {
          'app_id': context.app.appId,
          'platform': context.app.platform,
          'app_version': context.app.appVersion,
          'sdk_version': context.app.sdkVersion,
        },
        'user': {
          'anonymous_id': context.user.anonymousId,
          'user_id': context.user.userId,
          'traits': context.consent.piiAllowed ? context.user.traits : {},
        },
        'session': {
          'session_id': context.session.sessionId,
          'started_at': context.session.startedAt.toIso8601String(),
        },
        'privacy': {
          'analytics_allowed': context.consent.analyticsAllowed,
          'advertising_allowed': context.consent.advertisingAllowed,
          'pii_allowed': context.consent.piiAllowed,
        },
      },
    };
  }
}

class TrackResult {
  const TrackResult({
    required this.accepted,
    this.eventId,
    this.reason,
  });

  final bool accepted;
  final String? eventId;
  final String? reason;
}

class FlushResult {
  const FlushResult({
    required this.sent,
    required this.remaining,
  });

  final int sent;
  final int remaining;
}

class StayStartResult {
  const StayStartResult({
    required this.accepted,
    this.stayId,
    this.startedAt,
    this.reason,
  });

  final bool accepted;
  final String? stayId;
  final DateTime? startedAt;
  final String? reason;
}

class StayUpdateResult {
  const StayUpdateResult({
    required this.accepted,
    this.durationMs,
    this.reason,
  });

  final bool accepted;
  final int? durationMs;
  final String? reason;
}

class StayEndResult {
  const StayEndResult({
    required this.accepted,
    this.durationMs,
    this.stayId,
    this.reason,
  });

  final bool accepted;
  final int? durationMs;
  final String? stayId;
  final String? reason;
}

class StayState {
  StayState({
    required this.key,
    required this.stayId,
    required this.properties,
    required this.startedAt,
    required this.activeStartedAt,
  });

  final String key;
  final String stayId;
  final Map<String, Object?> properties;
  final DateTime startedAt;
  DateTime activeStartedAt;
  int accumulatedMs = 0;
  bool paused = false;
  DateTime? pausedAt;
}

bool _looksLikePii(String key) {
  return RegExp('email|phone|mobile|id_card|password|address', caseSensitive: false)
      .hasMatch(key);
}

String _defaultId() {
  final random = Random.secure();
  final timestamp = DateTime.now().microsecondsSinceEpoch.toRadixString(36);
  final suffix = List.generate(12, (_) => random.nextInt(36).toRadixString(36)).join();
  return 'ob_${timestamp}_$suffix';
}
