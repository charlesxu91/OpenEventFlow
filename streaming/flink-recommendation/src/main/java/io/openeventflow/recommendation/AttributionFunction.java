package io.openeventflow.recommendation;

import io.openeventflow.recommendation.model.Event;
import io.openeventflow.recommendation.model.TrainingSample;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class AttributionFunction extends KeyedProcessFunction<String, Event, TrainingSample> {
  public static final OutputTag<Event> LATE_EVENTS = new OutputTag<>("late-events") {};

  private final long windowMs;
  private final Set<String> positives;
  private transient MapState<String, Event> impressions;
  private transient MapState<String, Boolean> converted;
  private transient MapState<String, Boolean> seen;
  private transient MapState<String, Conversion> conversions;

  public AttributionFunction(long windowMs, Set<String> positives) {
    if (windowMs <= 0) throw new IllegalArgumentException("windowMs must be positive");
    this.windowMs = windowMs;
    this.positives = Set.copyOf(positives);
  }

  @Override
  public void open(Configuration ignored) {
    StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofMillis(windowMs * 4))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .build();
    impressions = state("impressions", String.class, Event.class, ttl);
    converted = state("converted", String.class, Boolean.class, ttl);
    seen = state("seen", String.class, Boolean.class, ttl);
    conversions = state("conversions", String.class, Conversion.class, ttl);
  }

  private <K, V> MapState<K, V> state(String name, Class<K> key, Class<V> value, StateTtlConfig ttl) {
    MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, key, value);
    descriptor.enableTimeToLive(ttl);
    return getRuntimeContext().getMapState(descriptor);
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<TrainingSample> out) throws Exception {
    long watermark = ctx.timerService().currentWatermark();
    if (watermark != Long.MIN_VALUE && event.timestamp() <= watermark) {
      ctx.output(LATE_EVENTS, event);
      return;
    }
    if (seen.contains(event.eventId())) return;
    seen.put(event.eventId(), true);

    if (event.isImpression()) {
      impressions.put(event.correlationKey(), event);
      converted.remove(event.correlationKey());
      ctx.timerService().registerEventTimeTimer(event.timestamp() + windowMs);
      return;
    }
    if (event.isRefund()) {
      Conversion conversion = findConversion(event);
      if (conversion != null) {
        out.collect(sample(conversion.impression(), event.eventType(), -1, event.timestamp(),
          conversion.sampleId(), event.eventId()));
      }
      return;
    }

    Event impression = impressions.get(event.correlationKey());
    if (impression == null || !isPositive(event.eventType())
        || event.timestamp() < impression.timestamp()
        || event.timestamp() > impression.timestamp() + windowMs) return;

    TrainingSample result = sample(impression, event.eventType(), 1, event.timestamp(), null, event.eventId());
    out.collect(result);
    converted.put(event.correlationKey(), true);
    Conversion conversion = new Conversion(result.sampleId(), impression);
    conversions.put(event.eventId(), conversion);
    if (event.orderId() != null) conversions.put("order:" + event.orderId(), conversion);
    if (event.paymentId() != null) conversions.put("payment:" + event.paymentId(), conversion);
  }

  private Conversion findConversion(Event event) throws Exception {
    if (event.conversionEventId() != null) {
      Conversion found = conversions.get(event.conversionEventId());
      if (found != null) return found;
    }
    if (event.paymentId() != null) {
      Conversion found = conversions.get("payment:" + event.paymentId());
      if (found != null) return found;
    }
    return event.orderId() == null ? null : conversions.get("order:" + event.orderId());
  }

  private boolean isPositive(String eventType) {
    if (positives.contains(eventType)) return true;
    String shortName = eventType.startsWith("recommendation_")
      ? eventType.substring("recommendation_".length()) : eventType;
    return positives.contains(shortName) || positives.contains("recommendation_" + shortName);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TrainingSample> out) throws Exception {
    List<String> expired = new ArrayList<>();
    for (Map.Entry<String, Event> entry : impressions.entries()) {
      Event impression = entry.getValue();
      if (impression.timestamp() + windowMs <= timestamp) {
        if (!Boolean.TRUE.equals(converted.get(entry.getKey()))) {
          out.collect(sample(impression, "expired", 0, timestamp, null, "expiry"));
        }
        expired.add(entry.getKey());
      }
    }
    for (String key : expired) {
      impressions.remove(key);
      converted.remove(key);
    }
  }

  private static TrainingSample sample(Event impression, String action, int label, long at,
                                       String correction, String suffix) {
    return new TrainingSample("sample:" + impression.eventId() + ":" + suffix,
      impression.userId(), impression.impressionId(), impression.itemType(), impression.itemId(),
      action, label, at, correction);
  }

  private record Conversion(String sampleId, Event impression) implements Serializable {}

  public static final class Transition {
    private final long window;
    private final Set<String> positives;
    private final Set<String> seen = new HashSet<>();
    private final Map<String, Event> impressions = new HashMap<>();
    private final Map<String, String> conversions = new HashMap<>();
    private final Set<String> converted = new HashSet<>();

    public Transition(long window, Set<String> positives) {
      this.window = window;
      this.positives = Set.copyOf(positives);
    }

    public List<TrainingSample> process(Event event) {
      if (!seen.add(event.eventId())) return List.of();
      if (event.isImpression()) {
        impressions.put(event.correlationKey(), event);
        return List.of();
      }
      if (event.isRefund()) {
        String original = conversions.get(event.conversionEventId());
        if (original == null) return List.of();
        Event impression = impressions.get(event.correlationKey());
        if (impression == null) return List.of();
        return List.of(sample(impression, event.eventType(), -1, event.timestamp(), original, event.eventId()));
      }
      Event impression = impressions.get(event.correlationKey());
      if (impression == null || event.timestamp() < impression.timestamp()
          || event.timestamp() > impression.timestamp() + window || !matches(positives, event.eventType())) {
        return List.of();
      }
      TrainingSample result = sample(impression, event.eventType(), 1, event.timestamp(), null, event.eventId());
      conversions.put(event.eventId(), result.sampleId());
      converted.add(event.correlationKey());
      return List.of(result);
    }

    public List<TrainingSample> advanceWatermark(long watermark) {
      List<TrainingSample> result = new ArrayList<>();
      Iterator<Map.Entry<String, Event>> iterator = impressions.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Event> entry = iterator.next();
        Event impression = entry.getValue();
        if (impression.timestamp() + window <= watermark) {
          if (!converted.contains(entry.getKey())) {
            result.add(sample(impression, "expired", 0, watermark, null, "expiry"));
          }
          iterator.remove();
        }
      }
      return result;
    }

    private static boolean matches(Set<String> positives, String type) {
      String shortName = type.startsWith("recommendation_") ? type.substring(15) : type;
      return positives.contains(type) || positives.contains(shortName) || positives.contains("recommendation_" + shortName);
    }
  }
}
