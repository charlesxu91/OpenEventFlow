import Foundation

public final class OpenEventFlowClient: Analytics {
    private let app: AppContext
    private let transport: AnalyticsTransport
    private let store: EventStore
    private let idGenerator: () -> String
    private let clock: () -> Date
    private var consent = AnalyticsConsent(analyticsAllowed: true)
    private var anonymousId: String
    private var userId: String?
    private var traits: [String: Any] = [:]
    private var session: SessionContext
    private var stays: [String: StayState] = [:]
    private let minStayDurationMs: Int64 = 1_000
    private let maxStayDurationMs: Int64 = 24 * 60 * 60 * 1_000

    public init(
        app: AppContext,
        transport: AnalyticsTransport,
        store: EventStore = MemoryEventStore(),
        idGenerator: @escaping () -> String = { UUID().uuidString },
        clock: @escaping () -> Date = Date.init
    ) {
        self.app = app
        self.transport = transport
        self.store = store
        self.idGenerator = idGenerator
        self.clock = clock
        self.anonymousId = idGenerator()
        self.session = SessionContext(sessionId: idGenerator(), startedAt: clock())
    }

    public func track(_ event: AnalyticsEvent) {
        guard consent.analyticsAllowed else { return }
        store.push(
            NormalizedEvent(
                eventId: idGenerator(),
                eventName: event.name,
                schema: event.schema,
                clientTime: clock(),
                properties: sanitize(event.properties),
                context: EventContext(
                    app: app,
                    user: UserContext(
                        anonymousId: anonymousId,
                        userId: userId,
                        traits: consent.piiAllowed ? traits : [:]
                    ),
                    session: session,
                    consent: consent
                )
            )
        )
    }

    public func screen(_ name: String, properties: [String: Any] = [:]) {
        var merged = properties
        merged["name"] = name
        track(
            SimpleAnalyticsEvent(
                name: "screen_view",
                schema: "iglu:io.openeventflow/screen_view/jsonschema/1-0-0",
                properties: merged
            )
        )
    }

    public func identify(userId: String, traits: [String: Any] = [:]) {
        self.userId = userId
        self.traits = traits
    }

    public func setConsent(_ consent: AnalyticsConsent) {
        self.consent = consent
    }

    public func flush() {
        let batch = store.peek(limit: 50)
        guard !batch.isEmpty else { return }
        transport.send(batch)
        store.remove(count: batch.count)
    }

    @discardableResult
    public func startNewSession() -> String {
        session = SessionContext(sessionId: idGenerator(), startedAt: clock())
        return session.sessionId
    }

    public func queueSize() -> Int {
        store.size()
    }

    @discardableResult
    public func beginStay(key: String, properties: [String: Any] = [:]) -> StayStartResult {
        precondition(!key.isEmpty, "stay key is required")
        let startedAt = clock()
        let stay = StayState(
            key: key,
            stayId: idGenerator(),
            properties: properties,
            startedAt: startedAt,
            activeStartedAt: startedAt
        )
        stays[key] = stay
        return StayStartResult(accepted: true, stayId: stay.stayId, startedAt: startedAt)
    }

    @discardableResult
    public func pauseStay(key: String) -> StayUpdateResult {
        guard var stay = stays[key] else {
            return StayUpdateResult(accepted: false, reason: "stay_not_found")
        }
        if stay.paused {
            return StayUpdateResult(accepted: true, durationMs: currentStayDuration(stay, now: clock()))
        }
        let now = clock()
        stay.accumulatedMs += millisBetween(stay.activeStartedAt, now)
        stay.paused = true
        stay.pausedAt = now
        stays[key] = stay
        return StayUpdateResult(accepted: true, durationMs: clampStayDuration(stay.accumulatedMs))
    }

    @discardableResult
    public func resumeStay(key: String) -> StayUpdateResult {
        guard var stay = stays[key] else {
            return StayUpdateResult(accepted: false, reason: "stay_not_found")
        }
        if !stay.paused {
            return StayUpdateResult(accepted: true, durationMs: currentStayDuration(stay, now: clock()))
        }
        stay.activeStartedAt = clock()
        stay.paused = false
        stay.pausedAt = nil
        stays[key] = stay
        return StayUpdateResult(accepted: true, durationMs: clampStayDuration(stay.accumulatedMs))
    }

    @discardableResult
    public func endStay(
        key: String,
        exitReason: String = "unknown",
        properties: [String: Any] = [:]
    ) -> StayEndResult {
        guard let stay = stays.removeValue(forKey: key) else {
            return StayEndResult(accepted: false, reason: "stay_not_found")
        }
        let durationMs = currentStayDuration(stay, now: clock())
        guard durationMs >= minStayDurationMs else {
            return StayEndResult(
                accepted: false,
                durationMs: durationMs,
                stayId: stay.stayId,
                reason: "stay_duration_below_minimum"
            )
        }
        guard consent.analyticsAllowed else {
            return StayEndResult(
                accepted: false,
                durationMs: durationMs,
                stayId: stay.stayId,
                reason: "analytics_consent_disabled"
            )
        }

        var merged = stay.properties
        for (key, value) in properties {
            merged[key] = value
        }
        merged["stay_id"] = stay.stayId
        merged["duration_ms"] = durationMs
        merged["exit_reason"] = exitReason
        track(
            SimpleAnalyticsEvent(
                name: "page_stay",
                schema: "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
                properties: merged
            )
        )
        return StayEndResult(accepted: true, durationMs: durationMs, stayId: stay.stayId)
    }

    @discardableResult
    public func switchStay(
        key: String,
        properties: [String: Any] = [:],
        exitReason: String = "route_change"
    ) -> StayEndResult {
        let ended = stays[key] == nil
            ? StayEndResult(accepted: false, reason: "stay_not_found")
            : endStay(key: key, exitReason: exitReason)
        beginStay(key: key, properties: properties)
        return ended
    }

    @discardableResult
    public func cancelStay(key: String) -> StayUpdateResult {
        StayUpdateResult(accepted: stays.removeValue(forKey: key) != nil)
    }

    @discardableResult
    public func flushActiveStays(exitReason: String = "app_shutdown") -> [StayEndResult] {
        Array(stays.keys).map { key in endStay(key: key, exitReason: exitReason) }
    }

    private func sanitize(_ properties: [String: Any]) -> [String: Any] {
        guard !consent.piiAllowed else { return properties }
        return properties.filter { key, _ in
            key.range(of: "email|phone|mobile|id_card|password|address", options: [.regularExpression, .caseInsensitive]) == nil
        }
    }

    private func currentStayDuration(_ stay: StayState, now: Date) -> Int64 {
        let activeDuration = stay.paused ? 0 : millisBetween(stay.activeStartedAt, now)
        return clampStayDuration(stay.accumulatedMs + activeDuration)
    }

    private func clampStayDuration(_ durationMs: Int64) -> Int64 {
        min(max(durationMs, 0), maxStayDurationMs)
    }

    private func millisBetween(_ start: Date, _ end: Date) -> Int64 {
        Int64((end.timeIntervalSince(start) * 1000).rounded(.down))
    }
}

public protocol AnalyticsTransport {
    func send(_ batch: [NormalizedEvent])
}

public protocol EventStore {
    func push(_ event: NormalizedEvent)
    func peek(limit: Int) -> [NormalizedEvent]
    func remove(count: Int)
    func size() -> Int
}

public final class MemoryEventStore: EventStore {
    private var events: [NormalizedEvent] = []

    public init() {}

    public func push(_ event: NormalizedEvent) {
        events.append(event)
    }

    public func peek(limit: Int) -> [NormalizedEvent] {
        Array(events.prefix(limit))
    }

    public func remove(count: Int) {
        events.removeFirst(Swift.min(count, events.count))
    }

    public func size() -> Int {
        events.count
    }
}

public struct AppContext {
    public let appId: String
    public let platform: String
    public let appVersion: String
    public let sdkVersion: String

    public init(appId: String, platform: String, appVersion: String, sdkVersion: String) {
        self.appId = appId
        self.platform = platform
        self.appVersion = appVersion
        self.sdkVersion = sdkVersion
    }
}

public struct UserContext {
    public let anonymousId: String
    public let userId: String?
    public let traits: [String: Any]
}

public struct SessionContext {
    public let sessionId: String
    public let startedAt: Date
}

public struct EventContext {
    public let app: AppContext
    public let user: UserContext
    public let session: SessionContext
    public let consent: AnalyticsConsent
}

public struct NormalizedEvent {
    public let eventId: String
    public let eventName: String
    public let schema: String
    public let clientTime: Date
    public let properties: [String: Any]
    public let context: EventContext
}

public struct SimpleAnalyticsEvent: AnalyticsEvent {
    public let name: String
    public let schema: String
    public let properties: [String: Any]

    public init(name: String, schema: String, properties: [String: Any]) {
        self.name = name
        self.schema = schema
        self.properties = properties
    }
}

public struct StayStartResult {
    public let accepted: Bool
    public let stayId: String?
    public let startedAt: Date?
    public let reason: String?

    public init(accepted: Bool, stayId: String? = nil, startedAt: Date? = nil, reason: String? = nil) {
        self.accepted = accepted
        self.stayId = stayId
        self.startedAt = startedAt
        self.reason = reason
    }
}

public struct StayUpdateResult {
    public let accepted: Bool
    public let durationMs: Int64?
    public let reason: String?

    public init(accepted: Bool, durationMs: Int64? = nil, reason: String? = nil) {
        self.accepted = accepted
        self.durationMs = durationMs
        self.reason = reason
    }
}

public struct StayEndResult {
    public let accepted: Bool
    public let durationMs: Int64?
    public let stayId: String?
    public let reason: String?

    public init(accepted: Bool, durationMs: Int64? = nil, stayId: String? = nil, reason: String? = nil) {
        self.accepted = accepted
        self.durationMs = durationMs
        self.stayId = stayId
        self.reason = reason
    }
}

private struct StayState {
    let key: String
    let stayId: String
    let properties: [String: Any]
    let startedAt: Date
    var activeStartedAt: Date
    var accumulatedMs: Int64 = 0
    var paused: Bool = false
    var pausedAt: Date?
}
