const {
  createInMemoryWarehouseAdapter,
  createWarehouseLoader
} = require("../../packages/warehouse/src/index");
const {
  createCollector,
  createInMemoryTopicBroker,
  createTrackingPlanRegistry
} = require("../../packages/collector/src/index");

class Warehouse {
  constructor() {
    this.adapter = createInMemoryWarehouseAdapter();
    this.loader = createWarehouseLoader({ adapter: this.adapter });
  }

  insert(tableName, row) {
    return this.adapter.insert(tableName, [row]);
  }

  async load(events) {
    return this.loader.load(events);
  }

  table(tableName) {
    return this.adapter.table(tableName);
  }
}

function createEcommerceWarehouseConsumer({ broker, warehouse, topic }) {
  let offset = 0;
  return {
    async drain() {
      const messages = broker.topic(topic).slice(offset);
      offset += messages.length;
      if (messages.length > 0) {
        await warehouse.load(messages);
      }
      return { consumed: messages.length };
    }
  };
}

module.exports = {
  InMemoryTopicBroker: createInMemoryTopicBroker,
  LocalCollector: createCollector,
  TrackingPlanRegistry: createTrackingPlanRegistry,
  Warehouse,
  createEcommerceWarehouseConsumer
};
