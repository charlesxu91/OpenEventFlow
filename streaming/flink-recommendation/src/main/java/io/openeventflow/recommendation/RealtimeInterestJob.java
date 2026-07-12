package io.openeventflow.recommendation;

import io.openeventflow.recommendation.InterestAggregateFunction.InterestProfile;
import io.openeventflow.recommendation.model.Event;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class RealtimeInterestJob {
  private RealtimeInterestJob() {}

  public static void main(String[] args) throws Exception {
    String brokers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    String featureTopic = env("FEATURE_TOPIC", "openeventflow_interest_profiles");
    StreamExecutionEnvironment environment = configuredEnvironment();
    profiles(environment, brokers)
      .map(JobSupport::json)
      .sinkTo(JobSupport.sink(brokers, featureTopic));
    environment.execute("OpenEventFlow realtime interest");
  }

  public static void run(FeatureSink sink) throws Exception {
    String brokers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    StreamExecutionEnvironment environment = configuredEnvironment();
    profiles(environment, brokers).addSink(new FeatureSinkOperator(sink)).name("feature-sink");
    environment.execute("OpenEventFlow realtime interest");
  }

  static DataStream<InterestProfile> profiles(StreamExecutionEnvironment environment, String brokers) {
    String input = env("INPUT_TOPIC", "snowplow_enriched_events");
    String parseErrorTopic = env("PARSE_ERROR_TOPIC", "openeventflow_parse_errors");
    long windowMs = Long.parseLong(env("INTEREST_WINDOW_MS", "60000"));

    var parsed = environment
      .fromSource(JobSupport.source(brokers, input, "openeventflow-interest"),
        WatermarkStrategy.noWatermarks(), "interest-events")
      .process(new EventJsonProcessFunction());
    parsed.getSideOutput(EventJsonProcessFunction.PARSE_ERRORS)
      .sinkTo(JobSupport.sink(brokers, parseErrorTopic));

    return parsed.assignTimestampsAndWatermarks(JobSupport.watermarks())
      .keyBy(Event::userId)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowMs)))
      .aggregate(new InterestAggregateFunction(Duration.ofDays(7).toMillis()));
  }

  private static StreamExecutionEnvironment configuredEnvironment() {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.enableCheckpointing(30_000);
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5_000));
    return environment;
  }

  private static String env(String name, String fallback) {
    return System.getenv().getOrDefault(name, fallback);
  }

  private static final class FeatureSinkOperator extends RichSinkFunction<InterestProfile> {
    private final FeatureSink sink;

    private FeatureSinkOperator(FeatureSink sink) {
      this.sink = sink;
    }

    @Override
    public void invoke(InterestProfile value, Context context) throws Exception {
      sink.write(value);
    }
  }
}
