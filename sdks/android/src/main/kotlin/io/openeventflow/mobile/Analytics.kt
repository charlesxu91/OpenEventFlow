package io.openeventflow.mobile

/**
 * Stable app-facing analytics API.
 *
 * The Snowplow adapter should live behind this boundary so business features do
 * not depend on Snowplow classes directly.
 */
interface Analytics {
    fun track(event: AnalyticsEvent)
    fun screen(name: String, properties: Map<String, Any?> = emptyMap())
    fun identify(userId: String, traits: Map<String, Any?> = emptyMap())
    fun setConsent(consent: AnalyticsConsent)
    fun flush()
}

interface AnalyticsEvent {
    val name: String
    val schema: String
    val properties: Map<String, Any?>
}

data class AnalyticsConsent(
    val analyticsAllowed: Boolean,
    val advertisingAllowed: Boolean = false,
    val piiAllowed: Boolean = false
)
