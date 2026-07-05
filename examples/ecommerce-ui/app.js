(function bootEcommerceUi() {
  const trackingPlan = {
    namespace: "io.openeventflow.app",
    schemaVendor: "io.openeventflow",
    events: [
      {
        name: "product_exposed",
        version: "1-0-0",
        required: ["product_id", "page", "position", "exposure_id"],
        properties: {
          product_id: { type: "string" },
          page: { type: "string" },
          position: { type: "integer" },
          exposure_id: { type: "string" },
          visible_ratio: { type: "number" },
          duration_ms: { type: "integer" },
          recommend_trace_id: { type: "string" }
        }
      },
      {
        name: "product_clicked",
        version: "1-0-0",
        required: ["product_id", "page", "position", "click_id"],
        properties: {
          product_id: { type: "string" },
          page: { type: "string" },
          position: { type: "integer" },
          click_id: { type: "string" },
          recommend_trace_id: { type: "string" }
        }
      },
      {
        name: "page_stay",
        version: "1-0-0",
        required: ["page", "duration_ms", "stay_id"],
        properties: {
          page: { type: "string" },
          duration_ms: { type: "integer" },
          stay_id: { type: "string" },
          exit_reason: { type: "string" }
        }
      },
      {
        name: "add_to_cart",
        version: "1-0-0",
        required: ["product_id", "sku_id", "quantity"],
        properties: {
          product_id: { type: "string" },
          sku_id: { type: "string" },
          quantity: { type: "integer" },
          price: { type: "number" },
          currency: { type: "string" }
        }
      }
    ]
  };

  const ids = ["evt-exp-ui", "evt-click-ui", "evt-stay-ui", "evt-cart-ui"];
  const times = [900, 1000, 1100, 1200, 1300];
  const broker = new OpenEventFlowBrowser.InMemoryTopicBroker();
  const registry = new OpenEventFlowBrowser.TrackingPlanRegistry(trackingPlan);
  const collector = new OpenEventFlowBrowser.LocalCollector({ broker, registry });
  const warehouse = new OpenEventFlowBrowser.Warehouse();
  const consumer = OpenEventFlowBrowser.createWarehouseConsumer({ broker, warehouse });
  const analytics = OpenEventFlowBrowser.createAnalytics({
    app: { appId: "shop-ui", platform: "web", appVersion: "1.0.0", sdkVersion: "0.1.0" },
    anonymousId: "anon-ui",
    idGenerator: () => ids.shift() || "evt-extra",
    clock: () => times.shift() || 1300,
    transport: { send: (batch) => collector.collect(batch) }
  });
  analytics.identify("user-ui-42", { segment: "vip" });

  async function flushToWarehouse() {
    await analytics.flush();
    consumer.drain();
    renderWarehouse();
  }

  async function trackExposure() {
    await analytics.track({
      name: "product_exposed",
      schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
      properties: {
        product_id: "sku-ui-100",
        page: "home_feed",
        position: 1,
        exposure_id: "exp-ui-100",
        visible_ratio: 0.91,
        duration_ms: 1500,
        recommend_trace_id: "rec-ui-abc"
      }
    });
    await flushToWarehouse();
  }

  async function trackClick() {
    document.querySelector("[data-testid='detail-panel']").hidden = false;
    await analytics.track({
      name: "product_clicked",
      schema: "iglu:io.openeventflow/product_clicked/jsonschema/1-0-0",
      properties: {
        product_id: "sku-ui-100",
        page: "home_feed",
        position: 1,
        click_id: "clk-ui-100",
        recommend_trace_id: "rec-ui-abc"
      }
    });
    await flushToWarehouse();
  }

  async function addToCart() {
    await analytics.track({
      name: "page_stay",
      schema: "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
      properties: {
        page: "product_detail",
        duration_ms: 8200,
        stay_id: "stay-ui-100",
        exit_reason: "add_to_cart"
      }
    });
    await analytics.track({
      name: "add_to_cart",
      schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
      properties: {
        product_id: "sku-ui-100",
        sku_id: "sku-ui-100-blue",
        quantity: 1,
        price: 88.5,
        currency: "CNY"
      }
    });
    await flushToWarehouse();
  }

  function renderWarehouse() {
    const snapshot = warehouse.snapshot();
    document.querySelector("[data-testid='warehouse-json']").textContent = JSON.stringify(snapshot, null, 2);
    document.querySelector("[data-testid='exposure-count']").textContent = String(snapshot.fact_product_exposures.length);
    document.querySelector("[data-testid='click-count']").textContent = String(snapshot.fact_product_clicks.length);
    document.querySelector("[data-testid='stay-count']").textContent = String(snapshot.fact_page_stays.length);
    document.querySelector("[data-testid='cart-count']").textContent = String(snapshot.fact_cart_adds.length);
  }

  window.__openBehaviorE2E__ = {
    broker,
    warehouse,
    snapshot: () => warehouse.snapshot()
  };

  document.querySelector("[data-testid='product-card']").addEventListener("click", trackClick);
  document.querySelector("[data-testid='add-to-cart']").addEventListener("click", addToCart);
  renderWarehouse();
  window.addEventListener("load", () => setTimeout(trackExposure, 50));
})();
