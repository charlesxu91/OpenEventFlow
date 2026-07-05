import Foundation

/// Stable app-facing analytics API.
///
/// The Snowplow adapter should live behind this boundary so business features do
/// not depend on Snowplow classes directly.
public protocol Analytics {
    func track(_ event: AnalyticsEvent)
    func screen(_ name: String, properties: [String: Any])
    func identify(userId: String, traits: [String: Any])
    func setConsent(_ consent: AnalyticsConsent)
    func flush()
}

public protocol AnalyticsEvent {
    var name: String { get }
    var schema: String { get }
    var properties: [String: Any] { get }
}

public struct AnalyticsConsent {
    public let analyticsAllowed: Bool
    public let advertisingAllowed: Bool
    public let piiAllowed: Bool

    public init(
        analyticsAllowed: Bool,
        advertisingAllowed: Bool = false,
        piiAllowed: Bool = false
    ) {
        self.analyticsAllowed = analyticsAllowed
        self.advertisingAllowed = advertisingAllowed
        self.piiAllowed = piiAllowed
    }
}
