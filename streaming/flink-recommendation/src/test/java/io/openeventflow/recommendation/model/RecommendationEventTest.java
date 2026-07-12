package io.openeventflow.recommendation.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RecommendationEventTest {

  @Test
  void storesFeaturesInAnIndependentMutableMapForFlinkSerialization() {
    Map<String, String> input = new HashMap<>(Map.of("category", "phones"));
    RecommendationEvent event = event(input);

    input.put("brand", "example");
    event.features().put("source", "collector");

    assertFalse(event.features().containsKey("brand"));
    assertEquals("collector", event.features().get("source"));
  }

  @Test
  void usesAnIndependentMutableMapWhenFeaturesAreMissing() {
    RecommendationEvent event = event(null);

    event.features().put("source", "collector");

    assertEquals("collector", event.features().get("source"));
  }

  private static RecommendationEvent event(Map<String, String> features) {
    return new RecommendationEvent(
        "event-1",
        RecommendationEvent.EventType.IMPRESSION,
        1L,
        "request-1",
        "impression-1",
        "product-1",
        "user-1",
        1,
        "model-v1",
        "features-v1",
        0L,
        features);
  }
}
