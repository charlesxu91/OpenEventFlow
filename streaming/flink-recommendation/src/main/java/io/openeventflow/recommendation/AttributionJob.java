package io.openeventflow.recommendation;

import io.openeventflow.recommendation.model.Event;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class AttributionJob {
  private AttributionJob() {}

  public static void main(String[] args) throws Exception {
    String brokers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    String input = env("INPUT_TOPIC", "snowplow_enriched_events");
    String output = env("TRAINING_TOPIC", "openeventflow_training_samples");
    String lateTopic = env("LATE_TOPIC", "openeventflow_late_events");
    String parseErrorTopic = env("PARSE_ERROR_TOPIC", "openeventflow_parse_errors");

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.enableCheckpointing(30_000);
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5_000));

    var parsed = environment
      .fromSource(JobSupport.source(brokers, input, "openeventflow-attribution"),
        WatermarkStrategy.noWatermarks(), "recommendation-events")
      .process(new EventJsonProcessFunction());
    parsed.getSideOutput(EventJsonProcessFunction.PARSE_ERRORS)
      .sinkTo(JobSupport.sink(brokers, parseErrorTopic));

    var events = parsed.assignTimestampsAndWatermarks(JobSupport.watermarks());
    var attributed = events
      .keyBy(Event::userId)
      .process(new AttributionFunction(Duration.ofHours(24).toMillis(), Set.of(
        "recommendation_click", "recommendation_add_to_cart", "recommendation_payment")));

    attributed.map(JobSupport::json).sinkTo(JobSupport.sink(brokers, output));
    attributed.getSideOutput(AttributionFunction.LATE_EVENTS)
      .map(JobSupport::json)
      .sinkTo(JobSupport.sink(brokers, lateTopic));
    environment.execute("OpenEventFlow recommendation attribution");
  }

  private static String env(String name, String fallback) {
    return System.getenv().getOrDefault(name, fallback);
  }
}
