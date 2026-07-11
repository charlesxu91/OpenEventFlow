package io.openeventflow.recommendation.core;

import io.openeventflow.recommendation.model.AttributionKey;
import io.openeventflow.recommendation.model.RecommendationEvent;
import io.openeventflow.recommendation.model.TrainingSample;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Event-time attribution core. Instances are single-threaded and partition-local. */
public final class AttributionProcessor {
  private final long windowMillis;
  private final long allowedLatenessMillis;
  private final Set<String> eventIds = new HashSet<>();
  private final Set<AttributionKey> finalized = new HashSet<>();
  private final Map<AttributionKey, List<RecommendationEvent>> events = new HashMap<>();

  public AttributionProcessor(Duration attributionWindow, Duration allowedLateness) {
    if (attributionWindow.isNegative() || attributionWindow.isZero() || allowedLateness.isNegative()) {
      throw new IllegalArgumentException("attributionWindow must be positive and allowedLateness non-negative");
    }
    this.windowMillis = attributionWindow.toMillis();
    this.allowedLatenessMillis = allowedLateness.toMillis();
  }

  /** Accepts an event once. Positive updates are emitted as upserts after an impression exists. */
  public List<TrainingSample> onEvent(RecommendationEvent event) {
    if (!eventIds.add(event.eventId()) || finalized.contains(event.attributionKey())) return List.of();
    List<RecommendationEvent> group = events.computeIfAbsent(event.attributionKey(), ignored -> new ArrayList<>());
    group.add(event);
    return build(group, false).map(List::of).orElseGet(List::of);
  }

  /** Finalizes both positive and negative samples once the event-time window is complete. */
  public List<TrainingSample> advanceWatermark(long watermarkMillis) {
    List<TrainingSample> output = new ArrayList<>();
    var iterator = events.entrySet().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      RecommendationEvent impression = impression(entry.getValue());
      if (impression != null && watermarkMillis >= deadline(impression)) {
        build(entry.getValue(), true).ifPresent(output::add);
        finalized.add(entry.getKey());
        iterator.remove();
      }
    }
    output.sort(Comparator.comparing(sample -> sample.key().toString()));
    return output;
  }

  private java.util.Optional<TrainingSample> build(List<RecommendationEvent> group, boolean isFinal) {
    RecommendationEvent impression = impression(group);
    if (impression == null) return java.util.Optional.empty();
    long end = impression.eventTimeMillis() + windowMillis;
    boolean click = false, favorite = false, cart = false, paid = false, cancelled = false, refund = false;
    long paidGmv = 0, refundedGmv = 0;
    for (RecommendationEvent event : group) {
      if (event.eventTimeMillis() < impression.eventTimeMillis() || event.eventTimeMillis() > end) continue;
      switch (event.eventType()) {
        case CLICK -> click = true;
        case FAVORITE -> favorite = true;
        case ADD_TO_CART -> cart = true;
        case ORDER_PAID -> { paid = true; paidGmv += event.grossMerchandiseValueMicros(); }
        case ORDER_CANCELLED -> cancelled = true;
        case REFUND -> { refund = true; refundedGmv += event.grossMerchandiseValueMicros(); }
        default -> { }
      }
    }
    return java.util.Optional.of(new TrainingSample(
        impression.attributionKey(), impression.userId(), impression.eventTimeMillis(), impression.position(),
        impression.modelVersion(), impression.featureSetVersion(), impression.features(), click, favorite, cart, paid,
        cancelled, refund, Math.max(0, paidGmv - refundedGmv), isFinal));
  }

  private long deadline(RecommendationEvent impression) {
    return Math.addExact(Math.addExact(impression.eventTimeMillis(), windowMillis), allowedLatenessMillis);
  }

  private static RecommendationEvent impression(List<RecommendationEvent> group) {
    return group.stream()
        .filter(event -> event.eventType() == RecommendationEvent.EventType.IMPRESSION)
        .min(Comparator.comparingLong(RecommendationEvent::eventTimeMillis))
        .orElse(null);
  }
}
