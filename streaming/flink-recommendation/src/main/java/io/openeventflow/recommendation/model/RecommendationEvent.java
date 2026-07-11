package io.openeventflow.recommendation.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public record RecommendationEvent(
    String eventId,
    EventType eventType,
    long eventTimeMillis,
    String requestId,
    String impressionId,
    String productId,
    String userId,
    int position,
    String modelVersion,
    String featureSetVersion,
    long grossMerchandiseValueMicros,
    Map<String, String> features) implements Serializable {

  public RecommendationEvent {
    eventId = require(eventId, "eventId");
    eventType = Objects.requireNonNull(eventType, "eventType");
    requestId = require(requestId, "requestId");
    impressionId = require(impressionId, "impressionId");
    productId = require(productId, "productId");
    userId = userId == null ? "" : userId;
    modelVersion = modelVersion == null ? "" : modelVersion;
    featureSetVersion = featureSetVersion == null ? "" : featureSetVersion;
    features = features == null ? Map.of() : Map.copyOf(features);
    if (eventTimeMillis < 0 || position < 0 || grossMerchandiseValueMicros < 0) {
      throw new IllegalArgumentException("timestamps, position and GMV must be non-negative");
    }
  }

  public AttributionKey attributionKey() {
    return new AttributionKey(requestId, impressionId, productId);
  }

  private static String require(String value, String name) {
    if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
    return value;
  }

  public enum EventType {
    DELIVERY, IMPRESSION, CLICK, FAVORITE, ADD_TO_CART, ORDER_PAID, ORDER_CANCELLED, REFUND
  }
}
