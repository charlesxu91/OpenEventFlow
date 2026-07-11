package io.openeventflow.recommendation.flink;

import io.openeventflow.recommendation.core.AttributionProcessor;
import io.openeventflow.recommendation.model.AttributionKey;
import io.openeventflow.recommendation.model.RecommendationEvent;
import io.openeventflow.recommendation.model.TrainingSample;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Checkpointed, rescalable event-time adapter around the deterministic attribution core. */
public final class AttributionProcessFunction
    extends KeyedProcessFunction<AttributionKey, RecommendationEvent, TrainingSample> {
  private final long windowMillis;
  private final long allowedLatenessMillis;
  private final long stateTtlMillis;
  private transient ValueState<ArrayList<RecommendationEvent>> events;
  private transient MapState<String, Boolean> seenEventIds;
  private transient ValueState<Long> deadline;

  public AttributionProcessFunction(Duration window, Duration allowedLateness) {
    this(window, allowedLateness, window.plus(allowedLateness).plus(Duration.ofDays(1)));
  }

  AttributionProcessFunction(Duration window, Duration allowedLateness, Duration stateTtl) {
    if (window.isZero() || window.isNegative() || allowedLateness.isNegative()) {
      throw new IllegalArgumentException("window must be positive and allowedLateness non-negative");
    }
    if (stateTtl.compareTo(window.plus(allowedLateness)) <= 0) {
      throw new IllegalArgumentException("stateTtl must exceed window plus allowedLateness");
    }
    this.windowMillis = window.toMillis();
    this.allowedLatenessMillis = allowedLateness.toMillis();
    this.stateTtlMillis = stateTtl.toMillis();
  }

  @Override public void open(Configuration parameters) {
    StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.milliseconds(stateTtlMillis))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupInRocksdbCompactFilter(1_000)
        .build();

    ValueStateDescriptor<ArrayList<RecommendationEvent>> eventsDescriptor = new ValueStateDescriptor<>(
        "attribution-events",
        TypeInformation.of(new TypeHint<ArrayList<RecommendationEvent>>() {}));
    eventsDescriptor.enableTimeToLive(ttl);
    events = getRuntimeContext().getState(eventsDescriptor);

    MapStateDescriptor<String, Boolean> dedupeDescriptor =
        new MapStateDescriptor<>("seen-event-ids", String.class, Boolean.class);
    dedupeDescriptor.enableTimeToLive(ttl);
    seenEventIds = getRuntimeContext().getMapState(dedupeDescriptor);

    ValueStateDescriptor<Long> deadlineDescriptor = new ValueStateDescriptor<>("attribution-deadline", Long.class);
    deadlineDescriptor.enableTimeToLive(ttl);
    deadline = getRuntimeContext().getState(deadlineDescriptor);
  }

  @Override public void processElement(RecommendationEvent event, Context context, Collector<TrainingSample> out)
      throws Exception {
    if (seenEventIds.contains(event.eventId())) return;
    seenEventIds.put(event.eventId(), Boolean.TRUE);

    ArrayList<RecommendationEvent> group = events.value();
    if (group == null) group = new ArrayList<>();
    group.add(event);
    events.update(group);

    TrainingSample update = evaluate(group, null);
    if (update != null) out.collect(update);

    Long nextDeadline = earliestDeadline(group);
    Long registeredDeadline = deadline.value();
    if (nextDeadline != null && !nextDeadline.equals(registeredDeadline)) {
      if (registeredDeadline != null) context.timerService().deleteEventTimeTimer(registeredDeadline);
      context.timerService().registerEventTimeTimer(nextDeadline);
      deadline.update(nextDeadline);
    }
  }

  @Override public void onTimer(long timestamp, OnTimerContext context, Collector<TrainingSample> out)
      throws Exception {
    Long registeredDeadline = deadline.value();
    if (registeredDeadline == null || timestamp != registeredDeadline) return;
    ArrayList<RecommendationEvent> group = events.value();
    if (group != null) {
      TrainingSample finalized = evaluate(group, timestamp);
      if (finalized != null) out.collect(finalized);
    }
    clearManagedState();
  }

  private TrainingSample evaluate(List<RecommendationEvent> group, Long watermark) {
    AttributionProcessor processor = new AttributionProcessor(
        Duration.ofMillis(windowMillis), Duration.ofMillis(allowedLatenessMillis));
    TrainingSample latest = null;
    for (RecommendationEvent event : group) {
      List<TrainingSample> updates = processor.onEvent(event);
      if (!updates.isEmpty()) latest = updates.get(updates.size() - 1);
    }
    if (watermark != null) {
      List<TrainingSample> finalized = processor.advanceWatermark(watermark);
      if (!finalized.isEmpty()) latest = finalized.get(finalized.size() - 1);
    }
    return latest;
  }

  private Long earliestDeadline(List<RecommendationEvent> group) {
    return group.stream()
        .filter(event -> event.eventType() == RecommendationEvent.EventType.IMPRESSION)
        .mapToLong(event -> Math.addExact(
            Math.addExact(event.eventTimeMillis(), windowMillis), allowedLatenessMillis))
        .min()
        .stream()
        .boxed()
        .findFirst()
        .orElse(null);
  }

  private void clearManagedState() throws Exception {
    events.clear();
    seenEventIds.clear();
    deadline.clear();
  }
}
