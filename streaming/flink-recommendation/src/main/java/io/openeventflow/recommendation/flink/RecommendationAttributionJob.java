package io.openeventflow.recommendation.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import io.openeventflow.recommendation.model.RecommendationEvent;
import io.openeventflow.recommendation.model.TrainingSample;
import java.time.Duration;
import java.time.Instant;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Runnable Kafka or NDJSON job entry. Kafka mode uses --bootstrap-servers, --input-topic, --output-topic and --group-id.
 * File mode uses --input and --output.
 */
public final class RecommendationAttributionJob {
  private RecommendationAttributionJob() {}

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    Duration window = durationParameter(parameters, "window-seconds", "window-hours", 168, true);
    Duration lateness = durationParameter(
        parameters, "allowed-lateness-seconds", "allowed-lateness-minutes", 10, false);
    Duration sourceIdleness = Duration.ofSeconds(parameters.getLong("source-idle-seconds", 60));
    if (sourceIdleness.isZero() || sourceIdleness.isNegative()) {
      throw new IllegalArgumentException("source-idle-seconds must be positive");
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureSerialization(env);
    env.getConfig().setGlobalJobParameters(parameters);
    DataStream<String> input = kafkaMode(parameters)
        ? kafkaInput(env, parameters)
        : env.readTextFile(parameters.getRequired("input"));
    DataStream<TrainingSample> samples = input.map(new JsonEventDecoder())
        .filter(Objects::nonNull)
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<RecommendationEvent>forBoundedOutOfOrderness(lateness)
            .withIdleness(sourceIdleness)
            .withTimestampAssigner((event, ignored) -> event.eventTimeMillis()))
        .keyBy(RecommendationEvent::attributionKey)
        .process(new AttributionProcessFunction(window, lateness));
    if (kafkaMode(parameters)) {
      samples.sinkTo(kafkaOutput(parameters));
    } else {
      samples.map(new JsonSampleEncoder()).writeAsText(parameters.getRequired("output"));
    }
    env.execute("OpenEventFlow recommendation attribution");
  }

  static void configureSerialization(StreamExecutionEnvironment environment) {
    // Flink 1.19's bundled Kryo FieldSerializer cannot reflectively serialize Java 17 records.
    // RecommendationEvent implements Serializable, so use Java serialization explicitly for
    // checkpointed attribution state instead of relying on the incompatible Kryo fallback.
    environment.getConfig().registerTypeWithKryoSerializer(RecommendationEvent.class, JavaSerializer.class);
  }

  static Duration durationParameter(
      ParameterTool parameters, String secondsKey, String legacyKey, long fallback, boolean legacyHours) {
    Duration value = parameters.has(secondsKey)
        ? Duration.ofSeconds(parameters.getLong(secondsKey))
        : (legacyHours
            ? Duration.ofHours(parameters.getLong(legacyKey, fallback))
            : Duration.ofMinutes(parameters.getLong(legacyKey, fallback)));
    if (value.isNegative() || (legacyHours && value.isZero())) {
      throw new IllegalArgumentException(secondsKey + " must be " + (legacyHours ? "positive" : "non-negative"));
    }
    return value;
  }

  static boolean kafkaMode(ParameterTool parameters) {
    boolean anyKafkaArgument = parameters.has("bootstrap-servers") || parameters.has("input-topic")
        || parameters.has("output-topic") || parameters.has("group-id");
    if (!anyKafkaArgument) return false;
    parameters.getRequired("bootstrap-servers");
    parameters.getRequired("input-topic");
    parameters.getRequired("output-topic");
    parameters.getRequired("group-id");
    return true;
  }

  private static DataStream<String> kafkaInput(StreamExecutionEnvironment env, ParameterTool parameters) {
    List<String> inputTopics = Arrays.stream(parameters.getRequired("input-topic").split(","))
        .map(String::trim)
        .filter(topic -> !topic.isEmpty())
        .toList();
    if (inputTopics.isEmpty()) throw new IllegalArgumentException("at least one input topic is required");
    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(parameters.getRequired("bootstrap-servers"))
        .setTopics(inputTopics)
        .setGroupId(parameters.getRequired("group-id"))
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "recommendation-events-kafka");
  }

  private static KafkaSink<TrainingSample> kafkaOutput(ParameterTool parameters) {
    return KafkaSink.<TrainingSample>builder()
        .setBootstrapServers(parameters.getRequired("bootstrap-servers"))
        .setRecordSerializer(new TrainingSampleKafkaSerializer(parameters.getRequired("output-topic")))
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  static final class JsonEventDecoder implements MapFunction<String, RecommendationEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Override public RecommendationEvent map(String json) throws Exception {
      JsonNode envelope = MAPPER.readTree(json);
      JsonNode properties = envelope.path("properties");
      RecommendationEvent.EventType type = eventType(text(envelope, "event_name"));
      if (type == null || !hasText(envelope, "event_id") || !hasText(properties, "request_id")
          || !hasText(properties, "impression_id") || !hasText(properties, "product_id")) {
        return null;
      }
      JsonNode user = envelope.path("context").path("user");
      String userId = firstText(user, "user_id", "anonymous_id");
      Map<String, String> features = new HashMap<>();
      copyFeature(properties, features, "surface");
      copyFeature(properties, features, "candidate_source");
      copyFeature(properties, features, "experiment_id");
      copyFeature(properties, features, "experiment_treatment");
      copyFeature(properties, features, "recommendation_generation");
      copyFeature(properties, features, "sku_id");
      return new RecommendationEvent(
          text(envelope, "event_id"), type, eventTime(firstEventTime(envelope)),
          text(properties, "request_id"), text(properties, "impression_id"), text(properties, "product_id"),
          userId, properties.path("position").asInt(0), text(properties, "model_version"),
          text(properties, "feature_set_version"), monetaryMicros(properties, type), features);
    }

    private static RecommendationEvent.EventType eventType(String name) {
      return switch (name) {
        case "recommendation_delivered" -> RecommendationEvent.EventType.DELIVERY;
        case "product_impressed", "product_exposed" -> RecommendationEvent.EventType.IMPRESSION;
        case "product_clicked" -> RecommendationEvent.EventType.CLICK;
        case "favorite_added" -> RecommendationEvent.EventType.FAVORITE;
        case "add_to_cart" -> RecommendationEvent.EventType.ADD_TO_CART;
        case "order_paid" -> RecommendationEvent.EventType.ORDER_PAID;
        case "order_cancelled" -> RecommendationEvent.EventType.ORDER_CANCELLED;
        case "order_refunded" -> RecommendationEvent.EventType.REFUND;
        default -> null;
      };
    }

    private static long eventTime(JsonNode value) {
      if (value.isIntegralNumber()) return value.asLong();
      if (value.isTextual()) return Instant.parse(value.asText()).toEpochMilli();
      throw new IllegalArgumentException(
          "client_time, timestamp, or collector_time must be epoch milliseconds or ISO-8601");
    }

    private static JsonNode firstEventTime(JsonNode envelope) {
      for (String field : List.of("client_time", "timestamp", "collector_time")) {
        JsonNode value = envelope.path(field);
        if (!value.isMissingNode() && !value.isNull()) return value;
      }
      return MAPPER.missingNode();
    }

    private static long monetaryMicros(JsonNode properties, RecommendationEvent.EventType type) {
      String field = switch (type) {
        case ORDER_PAID -> "paid_amount";
        case REFUND -> "refund_amount";
        default -> null;
      };
      if (field == null || !properties.path(field).isNumber()) return 0;
      return properties.path(field).decimalValue().multiply(BigDecimal.valueOf(1_000_000)).longValueExact();
    }

    private static void copyFeature(JsonNode source, Map<String, String> target, String field) {
      if (hasText(source, field)) target.put(field, text(source, field));
    }

    private static String firstText(JsonNode source, String first, String second) {
      return hasText(source, first) ? text(source, first) : text(source, second);
    }

    private static boolean hasText(JsonNode source, String field) {
      return source.hasNonNull(field) && !source.path(field).asText().isBlank();
    }

    private static String text(JsonNode source, String field) {
      return source.path(field).asText("");
    }
  }

  static final class JsonSampleEncoder implements MapFunction<io.openeventflow.recommendation.model.TrainingSample, String> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Override public String map(io.openeventflow.recommendation.model.TrainingSample sample) throws Exception {
      return MAPPER.writeValueAsString(sample);
    }
  }

  static final class TrainingSampleKafkaSerializer implements KafkaRecordSerializationSchema<TrainingSample> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String topic;

    TrainingSampleKafkaSerializer(String topic) {
      if (topic == null || topic.isBlank()) throw new IllegalArgumentException("output topic is required");
      this.topic = topic;
    }

    @Override public ProducerRecord<byte[], byte[]> serialize(
        TrainingSample sample, KafkaSinkContext context, Long timestamp) {
      try {
        byte[] key = sample.key().toString().getBytes(StandardCharsets.UTF_8);
        byte[] value = MAPPER.writeValueAsBytes(sample);
        return new ProducerRecord<>(topic, null, timestamp, key, value);
      } catch (Exception error) {
        throw new IllegalArgumentException("training sample cannot be serialized", error);
      }
    }
  }
}
