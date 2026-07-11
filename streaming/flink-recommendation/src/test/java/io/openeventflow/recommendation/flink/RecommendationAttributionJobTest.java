package io.openeventflow.recommendation.flink;

import static org.junit.jupiter.api.Assertions.*;

import io.openeventflow.recommendation.model.AttributionKey;
import io.openeventflow.recommendation.model.RecommendationEvent;
import io.openeventflow.recommendation.model.TrainingSample;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

final class RecommendationAttributionJobTest {
  @Test void detectsFileAndCompleteKafkaModes() {
    assertFalse(RecommendationAttributionJob.kafkaMode(ParameterTool.fromArgs(
        new String[] {"--input", "events", "--output", "samples"})));
    assertTrue(RecommendationAttributionJob.kafkaMode(ParameterTool.fromArgs(new String[] {
        "--bootstrap-servers", "kafka:9092", "--input-topic", "events", "--output-topic", "samples",
        "--group-id", "attribution"})));
    assertThrows(RuntimeException.class, () -> RecommendationAttributionJob.kafkaMode(
        ParameterTool.fromArgs(new String[] {"--bootstrap-servers", "kafka:9092"})));
  }

  @Test void decodesEventJson() throws Exception {
    String json = """
        {"event_id":"event-1","event_name":"product_impressed","client_time":1000,
         "properties":{"request_id":"request-1","impression_id":"impression-1","product_id":"product-1",
         "position":2,"model_version":"rank-v1","feature_set_version":"features-v1","surface":"HOME"},
         "context":{"user":{"user_id":"user-1","anonymous_id":"anonymous-1"}}}
        """;
    RecommendationEvent event = new RecommendationAttributionJob.JsonEventDecoder().map(json);
    assertEquals("event-1", event.eventId());
    assertEquals(RecommendationEvent.EventType.IMPRESSION, event.eventType());
    assertEquals("user-1", event.userId());
    assertEquals("HOME", event.features().get("surface"));
  }

  @Test void mapsAuthoritativeMoneyAndFiltersUnattributableEvents() throws Exception {
    String paid = """
        {"event_id":"paid-1","event_name":"order_paid","client_time":"2026-07-11T12:00:00Z",
         "properties":{"request_id":"r","impression_id":"i","product_id":"p","paid_amount":12.34},
         "context":{"user":{"anonymous_id":"anon"}}}
        """;
    RecommendationAttributionJob.JsonEventDecoder decoder = new RecommendationAttributionJob.JsonEventDecoder();
    RecommendationEvent event = decoder.map(paid);
    assertEquals(12_340_000, event.grossMerchandiseValueMicros());
    assertEquals("anon", event.userId());
    assertNull(decoder.map("""
        {"event_id":"page-1","event_name":"page_viewed","client_time":1000,"properties":{},"context":{}}
        """));
    assertNull(decoder.map("""
        {"event_id":"click-1","event_name":"product_clicked","client_time":1000,
         "properties":{"product_id":"p"},"context":{}}
        """));
  }

  @Test void serializesStableAttributionKeyAndJsonValue() {
    AttributionKey key = new AttributionKey("request-1", "impression-1", "product-1");
    TrainingSample sample = new TrainingSample(key, "user-1", 1000, 2, "rank-v1", "features-v1",
        Map.of("category", "phones"), true, false, false, false, false, false, 0, false);
    var record = new RecommendationAttributionJob.TrainingSampleKafkaSerializer("training-samples")
        .serialize(sample, null, 1234L);
    assertEquals("training-samples", record.topic());
    assertEquals(key.toString(), new String(record.key(), StandardCharsets.UTF_8));
    assertTrue(new String(record.value(), StandardCharsets.UTF_8).contains("\"clicked\":true"));
    assertEquals(1234L, record.timestamp());
  }
}
