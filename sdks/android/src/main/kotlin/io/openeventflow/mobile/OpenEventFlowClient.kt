package io.openeventflow.mobile

import java.util.UUID

class OpenEventFlowClient(
    private val app: AppContext,
    private val transport: AnalyticsTransport,
    private val store: EventStore = MemoryEventStore(),
    private val idGenerator: () -> String = { UUID.randomUUID().toString() },
    private val clock: () -> Long = { System.currentTimeMillis() }
) : Analytics {
    private var consent = AnalyticsConsent(analyticsAllowed = true)
    private var anonymousId = idGenerator()
    private var userId: String? = null
    private var traits: Map<String, Any?> = emptyMap()
    private var session = SessionContext(sessionId = idGenerator(), startedAt = clock())
    private val stays = mutableMapOf<String, StayState>()
    private val minStayDurationMs = 1_000L
    private val maxStayDurationMs = 24 * 60 * 60 * 1_000L

    override fun track(event: AnalyticsEvent) {
        if (!consent.analyticsAllowed) {
            return
        }
        store.push(
            NormalizedEvent(
                eventId = idGenerator(),
                eventName = event.name,
                schema = event.schema,
                clientTime = clock(),
                properties = sanitize(event.properties),
                context = EventContext(
                    app = app,
                    user = UserContext(
                        anonymousId = anonymousId,
                        userId = userId,
                        traits = if (consent.piiAllowed) traits else emptyMap()
                    ),
                    session = session,
                    consent = consent
                )
            )
        )
    }

    override fun screen(name: String, properties: Map<String, Any?>) {
        track(
            SimpleAnalyticsEvent(
                name = "screen_view",
                schema = "iglu:io.openeventflow/screen_view/jsonschema/1-0-0",
                properties = mapOf("name" to name) + properties
            )
        )
    }

    override fun identify(userId: String, traits: Map<String, Any?>) {
        this.userId = userId
        this.traits = traits
    }

    override fun setConsent(consent: AnalyticsConsent) {
        this.consent = consent
    }

    override fun flush() {
        val batch = store.peek(50)
        if (batch.isEmpty()) {
            return
        }
        transport.send(batch)
        store.remove(batch.size)
    }

    fun startNewSession(): String {
        session = SessionContext(sessionId = idGenerator(), startedAt = clock())
        return session.sessionId
    }

    fun queueSize(): Int = store.size()

    fun beginStay(key: String, properties: Map<String, Any?> = emptyMap()): StayStartResult {
        require(key.isNotBlank()) { "stay key is required" }
        val startedAt = clock()
        val stay = StayState(
            key = key,
            stayId = idGenerator(),
            properties = properties,
            startedAt = startedAt,
            activeStartedAt = startedAt
        )
        stays[key] = stay
        return StayStartResult(accepted = true, stayId = stay.stayId, startedAt = startedAt)
    }

    fun pauseStay(key: String): StayUpdateResult {
        val stay = stays[key] ?: return StayUpdateResult(accepted = false, reason = "stay_not_found")
        if (stay.paused) {
            return StayUpdateResult(accepted = true, durationMs = currentStayDuration(stay, clock()))
        }
        val now = clock()
        stay.accumulatedMs += now - stay.activeStartedAt
        stay.paused = true
        stay.pausedAt = now
        return StayUpdateResult(accepted = true, durationMs = clampStayDuration(stay.accumulatedMs))
    }

    fun resumeStay(key: String): StayUpdateResult {
        val stay = stays[key] ?: return StayUpdateResult(accepted = false, reason = "stay_not_found")
        if (!stay.paused) {
            return StayUpdateResult(accepted = true, durationMs = currentStayDuration(stay, clock()))
        }
        stay.activeStartedAt = clock()
        stay.paused = false
        stay.pausedAt = null
        return StayUpdateResult(accepted = true, durationMs = clampStayDuration(stay.accumulatedMs))
    }

    fun endStay(
        key: String,
        exitReason: String = "unknown",
        properties: Map<String, Any?> = emptyMap()
    ): StayEndResult {
        val stay = stays.remove(key) ?: return StayEndResult(accepted = false, reason = "stay_not_found")
        val durationMs = currentStayDuration(stay, clock())
        if (durationMs < minStayDurationMs) {
            return StayEndResult(
                accepted = false,
                durationMs = durationMs,
                stayId = stay.stayId,
                reason = "stay_duration_below_minimum"
            )
        }
        if (!consent.analyticsAllowed) {
            return StayEndResult(
                accepted = false,
                durationMs = durationMs,
                stayId = stay.stayId,
                reason = "analytics_consent_disabled"
            )
        }
        track(
            SimpleAnalyticsEvent(
                name = "page_stay",
                schema = "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
                properties = stay.properties + properties + mapOf(
                    "stay_id" to stay.stayId,
                    "duration_ms" to durationMs,
                    "exit_reason" to exitReason
                )
            )
        )
        return StayEndResult(accepted = true, durationMs = durationMs, stayId = stay.stayId)
    }

    fun switchStay(
        key: String,
        properties: Map<String, Any?> = emptyMap(),
        exitReason: String = "route_change"
    ): StayEndResult {
        val ended = if (stays.containsKey(key)) {
            endStay(key, exitReason = exitReason)
        } else {
            StayEndResult(accepted = false, reason = "stay_not_found")
        }
        beginStay(key, properties)
        return ended
    }

    fun cancelStay(key: String): StayUpdateResult {
        return StayUpdateResult(accepted = stays.remove(key) != null)
    }

    fun flushActiveStays(exitReason: String = "app_shutdown"): List<StayEndResult> {
        return stays.keys.toList().map { key -> endStay(key, exitReason = exitReason) }
    }

    private fun sanitize(properties: Map<String, Any?>): Map<String, Any?> {
        if (consent.piiAllowed) {
            return properties
        }
        return properties.filterKeys { !it.contains(Regex("email|phone|mobile|id_card|password|address", RegexOption.IGNORE_CASE)) }
    }

    private fun currentStayDuration(stay: StayState, now: Long): Long {
        val activeDuration = if (stay.paused) 0 else now - stay.activeStartedAt
        return clampStayDuration(stay.accumulatedMs + activeDuration)
    }

    private fun clampStayDuration(durationMs: Long): Long {
        return durationMs.coerceAtLeast(0).coerceAtMost(maxStayDurationMs)
    }
}

interface AnalyticsTransport {
    fun send(batch: List<NormalizedEvent>)
}

interface EventStore {
    fun push(event: NormalizedEvent)
    fun peek(limit: Int): List<NormalizedEvent>
    fun remove(count: Int)
    fun size(): Int
}

class MemoryEventStore : EventStore {
    private val events = mutableListOf<NormalizedEvent>()

    override fun push(event: NormalizedEvent) {
        events.add(event)
    }

    override fun peek(limit: Int): List<NormalizedEvent> = events.take(limit)

    override fun remove(count: Int) {
        repeat(count.coerceAtMost(events.size)) {
            events.removeAt(0)
        }
    }

    override fun size(): Int = events.size
}

data class AppContext(
    val appId: String,
    val platform: String,
    val appVersion: String,
    val sdkVersion: String
)

data class UserContext(
    val anonymousId: String,
    val userId: String?,
    val traits: Map<String, Any?>
)

data class SessionContext(
    val sessionId: String,
    val startedAt: Long
)

data class EventContext(
    val app: AppContext,
    val user: UserContext,
    val session: SessionContext,
    val consent: AnalyticsConsent
)

data class NormalizedEvent(
    val eventId: String,
    val eventName: String,
    val schema: String,
    val clientTime: Long,
    val properties: Map<String, Any?>,
    val context: EventContext
)

data class SimpleAnalyticsEvent(
    override val name: String,
    override val schema: String,
    override val properties: Map<String, Any?>
) : AnalyticsEvent

data class StayStartResult(
    val accepted: Boolean,
    val stayId: String? = null,
    val startedAt: Long? = null,
    val reason: String? = null
)

data class StayUpdateResult(
    val accepted: Boolean,
    val durationMs: Long? = null,
    val reason: String? = null
)

data class StayEndResult(
    val accepted: Boolean,
    val durationMs: Long? = null,
    val stayId: String? = null,
    val reason: String? = null
)

private data class StayState(
    val key: String,
    val stayId: String,
    val properties: Map<String, Any?>,
    val startedAt: Long,
    var activeStartedAt: Long,
    var accumulatedMs: Long = 0,
    var paused: Boolean = false,
    var pausedAt: Long? = null
)
