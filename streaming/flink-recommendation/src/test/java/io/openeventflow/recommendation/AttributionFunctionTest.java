package io.openeventflow.recommendation;

import io.openeventflow.recommendation.model.Event;
import io.openeventflow.recommendation.model.TrainingSample;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class AttributionFunctionTest {
  @Test void parsesTrackingContractJson() throws Exception {
    var event = EventJsonParser.parse("""
      {"event_id":"i1","user_id":"u1","event_name":"recommendation_impression",
       "impression_id":"imp1","item_type":"product","item_id":"p1","timestamp":100}
      """);
    assertEquals("i1", event.eventId());
    assertEquals("recommendation_impression", event.eventType());
    assertEquals("imp1", event.impressionId());
  }

  @Test void realOperatorEmitsMultiplePositivesAndRefundCorrection() throws Exception {
    try (var harness = harness(1_000, Set.of("recommendation_click", "recommendation_payment"))) {
      harness.processElement(Event.impression("i1", "u1", "imp1", "product", "p1", 100), 100);
      harness.processElement(Event.action("c1", "u1", "recommendation_click", "imp1", "product", "p1", 200), 200);
      harness.processElement(Event.payment("pay-event", "u1", "imp1", "product", "p1", "order-1", "pay-1", 300), 300);
      harness.processElement(Event.refundByPayment("r1", "u1", "imp1", "product", "p1", "pay-1", 350), 350);
      List<TrainingSample> samples = harness.extractOutputValues();
      assertEquals(List.of(1, 1, -1), samples.stream().map(TrainingSample::label).toList());
      assertEquals(samples.get(1).sampleId(), samples.get(2).correctsSampleId());
    }
  }

  @Test void realOperatorRejectsOutsideWindowAndRoutesLateEvents() throws Exception {
    try (var harness = harness(1_000, Set.of("recommendation_click"))) {
      harness.processElement(Event.impression("i1", "u1", "imp1", "product", "p1", 100), 100);
      harness.processElement(Event.action("too-late", "u1", "recommendation_click", "imp1", "product", "p1", 1_101), 1_101);
      assertTrue(harness.extractOutputValues().isEmpty());
      harness.processWatermark(new Watermark(2_000));
      harness.processElement(Event.action("late", "u1", "recommendation_click", "imp1", "product", "p1", 2_000), 2_000);
      var late = harness.getSideOutput(AttributionFunction.LATE_EVENTS);
      assertNotNull(late);
      assertEquals("late", ((StreamRecord<Event>) late.peek()).getValue().eventId());
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TrainingSample> harness(long window, Set<String> positives) throws Exception {
    var harness = new KeyedOneInputStreamOperatorTestHarness<>(
      new KeyedProcessOperator<>(new AttributionFunction(window, positives)),
      Event::userId, Types.STRING);
    harness.open();
    return harness;
  }

  @Test void deduplicatesAndEmitsPositiveAndNegativeSamples() {
    var attribution = new AttributionFunction.Transition(1_000, Set.of("click"));
    var impression = Event.impression("i1", "u1", "imp1", "product", "p1", 100);
    assertTrue(attribution.process(impression).isEmpty());
    assertTrue(attribution.process(impression).isEmpty());
    var positive = attribution.process(Event.action("c1", "u1", "click", "imp1", "product", "p1", 200));
    assertEquals(1, positive.size()); assertEquals(1, positive.get(0).label());
    attribution.process(Event.impression("i2", "u1", "imp2", "content", "x", 300));
    var expired = attribution.advanceWatermark(1_301);
    assertEquals(1, expired.size()); assertEquals(0, expired.get(0).label());
  }

  @Test void emitsRefundCorrectionReferencingConversion() {
    var attribution = new AttributionFunction.Transition(1_000, Set.of("payment"));
    attribution.process(Event.impression("i1", "u1", "imp1", "product", "p1", 100));
    var conversion = attribution.process(Event.action("pay1", "u1", "payment", "imp1", "product", "p1", 200)).get(0);
    var correction = attribution.process(Event.refund("r1", "u1", "imp1", "product", "p1", "pay1", 250)).get(0);
    assertEquals(-1, correction.label()); assertEquals(conversion.sampleId(), correction.correctsSampleId());
  }
}
