package io.openeventflow.recommendation;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

final class JobSupport {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JobSupport() {}

  static KafkaSource<String> source(String brokers, String topic, String groupId) {
    return KafkaSource.<String>builder()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();
  }

  static KafkaSink<String> sink(String brokers, String topic) {
    return KafkaSink.<String>builder()
      .setBootstrapServers(brokers)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .build();
  }

  static WatermarkStrategy<io.openeventflow.recommendation.model.Event> watermarks() {
    return WatermarkStrategy.<io.openeventflow.recommendation.model.Event>forBoundedOutOfOrderness(Duration.ofSeconds(30))
      .withTimestampAssigner((event, previous) -> event.timestamp())
      .withIdleness(Duration.ofMinutes(1));
  }

  static String json(Object value) throws Exception {
    return MAPPER.writeValueAsString(value);
  }
}
