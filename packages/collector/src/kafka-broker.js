function createKafkaTopicBroker(options = {}) {
  const kafkaModule = options.kafkaModule || loadKafkaJs();
  const brokers = options.brokers || splitCsv(process.env.KAFKA_BROKERS);
  if (!options.kafka && brokers.length === 0) {
    throw new Error("Kafka brokers are required");
  }

  const kafka = options.kafka || new kafkaModule.Kafka({
    clientId: options.clientId || process.env.KAFKA_CLIENT_ID || "openeventflow-collector",
    brokers,
    ssl: options.ssl,
    sasl: options.sasl,
    connectionTimeout: options.connectionTimeout || 5000,
    requestTimeout: options.requestTimeout || 30000
  });
  const producer = options.producer || kafka.producer({
    idempotent: options.idempotent !== false,
    maxInFlightRequests: options.maxInFlightRequests || 5,
    allowAutoTopicCreation: options.allowAutoTopicCreation === true,
    transactionTimeout: options.transactionTimeout || 30000
  });
  const compression = resolveCompression(kafkaModule, options.compression || "gzip");
  let connectPromise;
  let connected = false;
  let closed = false;

  async function connect() {
    if (closed) throw new Error("Kafka broker is closed");
    if (!connectPromise) {
      connectPromise = Promise.resolve(producer.connect()).then(() => {
        connected = true;
      }).catch((error) => {
        connectPromise = undefined;
        throw error;
      });
    }
    await connectPromise;
  }

  return {
    async publish(topic, message, key) {
      return this.publishBatch([{ topic, message, key }]);
    },
    async publishBatch(records) {
      if (!Array.isArray(records) || records.length === 0) return;
      await connect();
      const grouped = new Map();
      for (const record of records) {
        if (!grouped.has(record.topic)) grouped.set(record.topic, []);
        grouped.get(record.topic).push({
          key: record.key == null ? null : String(record.key),
          value: JSON.stringify(record.message),
          headers: { "content-type": "application/json" }
        });
      }
      await producer.sendBatch({
        acks: -1,
        compression,
        topicMessages: Array.from(grouped, ([topic, messages]) => ({ topic, messages }))
      });
    },
    async health() {
      if (closed) return { ready: false, reason: "kafka_broker_closed" };
      try {
        await connect();
        return { ready: true };
      } catch (error) {
        return { ready: false, reason: "kafka_unavailable", message: error.message };
      }
    },
    async close() {
      if (closed) return;
      closed = true;
      if (connectPromise) {
        try { await connectPromise; } catch (_) { /* no connection to close */ }
      }
      if (connected) await producer.disconnect();
      connected = false;
    }
  };
}

function loadKafkaJs() {
  try {
    return require("kafkajs");
  } catch (error) {
    throw new Error("kafkajs is required when Kafka broker is enabled", { cause: error });
  }
}

function splitCsv(value) {
  return String(value || "").split(",").map((item) => item.trim()).filter(Boolean);
}

function resolveCompression(kafkaModule, value) {
  const names = { gzip: "GZIP", snappy: "Snappy", lz4: "LZ4", zstd: "ZSTD", none: "None" };
  const name = names[String(value).toLowerCase()];
  if (!name || kafkaModule.CompressionTypes[name] === undefined) {
    throw new Error(`Unsupported Kafka compression: ${value}`);
  }
  return kafkaModule.CompressionTypes[name];
}

module.exports = { createKafkaTopicBroker };
