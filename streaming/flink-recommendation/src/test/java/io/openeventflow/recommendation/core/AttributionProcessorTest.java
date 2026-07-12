package io.openeventflow.recommendation.core;

import static io.openeventflow.recommendation.model.RecommendationEvent.EventType.*;
import static org.junit.jupiter.api.Assertions.*;

import io.openeventflow.recommendation.model.RecommendationEvent;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class AttributionProcessorTest {
  private final AttributionProcessor processor = new AttributionProcessor(Duration.ofHours(1), Duration.ofMinutes(5));

  @Test void attributesOutOfOrderActionsAndFinalizesLabels() {
    assertTrue(processor.onEvent(event("click", CLICK, 1_100, 0)).isEmpty());
    var update = processor.onEvent(event("imp", IMPRESSION, 1_000, 0));
    assertEquals(1, update.size());
    assertTrue(update.get(0).clicked());
    assertFalse(update.get(0).finalized());

    processor.onEvent(event("cart", ADD_TO_CART, 1_200, 0));
    processor.onEvent(event("favorite", FAVORITE, 1_150, 0));
    processor.onEvent(event("paid", ORDER_PAID, 1_300, 900_000));
    processor.onEvent(event("cancelled", ORDER_CANCELLED, 1_350, 0));
    processor.onEvent(event("refund", REFUND, 1_400, 200_000));
    var result = processor.advanceWatermark(1_000 + Duration.ofHours(1).toMillis() + Duration.ofMinutes(5).toMillis());
    assertEquals(1, result.size());
    assertAll(
        () -> assertTrue(result.get(0).clicked()),
        () -> assertTrue(result.get(0).favorited()),
        () -> assertTrue(result.get(0).addedToCart()),
        () -> assertTrue(result.get(0).paid()),
        () -> assertTrue(result.get(0).cancelled()),
        () -> assertTrue(result.get(0).refunded()),
        () -> assertEquals(700_000, result.get(0).netGmvMicros()),
        () -> assertTrue(result.get(0).finalized()));
  }

  @Test void deduplicatesByEventId() {
    processor.onEvent(event("imp", IMPRESSION, 1_000, 0));
    assertEquals(1, processor.onEvent(event("click", CLICK, 1_100, 0)).size());
    assertTrue(processor.onEvent(event("click", CLICK, 1_100, 0)).isEmpty());
  }

  @Test void emitsNegativeSampleOnlyAfterWindowCloses() {
    processor.onEvent(event("imp", IMPRESSION, 1_000, 0));
    assertTrue(processor.advanceWatermark(2_000).isEmpty());
    var result = processor.advanceWatermark(1_000 + Duration.ofHours(1).toMillis() + Duration.ofMinutes(5).toMillis());
    assertEquals(1, result.size());
    assertFalse(result.get(0).clicked());
    assertFalse(result.get(0).paid());
    assertTrue(result.get(0).finalized());
  }

  @Test void ignoresActionsOutsideAttributionWindow() {
    processor.onEvent(event("imp", IMPRESSION, 1_000, 0));
    processor.onEvent(event("late", CLICK, 1_001 + Duration.ofHours(1).toMillis(), 0));
    var result = processor.advanceWatermark(1_000 + Duration.ofHours(1).toMillis() + Duration.ofMinutes(5).toMillis());
    assertFalse(result.get(0).clicked());
  }

  private static RecommendationEvent event(String id, RecommendationEvent.EventType type, long time, long gmv) {
    return new RecommendationEvent(id, type, time, "request-1", "impression-1", "product-1", "user-1", 2,
        "rank-v1", "features-v3", gmv, Map.of("category", "phones"));
  }
}
