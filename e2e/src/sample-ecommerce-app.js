async function runEcommerceJourney(analytics) {
  analytics.identify("user-42", { segment: "vip" });

  await analytics.track({
    name: "product_exposed",
    schema: "iglu:io.openeventflow/product_exposed/jsonschema/1-0-0",
    properties: {
      product_id: "sku-100",
      page: "home_feed",
      position: 2,
      exposure_id: "exp-100",
      visible_ratio: 0.83,
      duration_ms: 1200,
      recommend_trace_id: "rec-abc"
    }
  });

  await analytics.track({
    name: "product_clicked",
    schema: "iglu:io.openeventflow/product_clicked/jsonschema/1-0-0",
    properties: {
      product_id: "sku-100",
      page: "home_feed",
      position: 2,
      click_id: "clk-100",
      recommend_trace_id: "rec-abc"
    }
  });

  await analytics.track({
    name: "page_stay",
    schema: "iglu:io.openeventflow/page_stay/jsonschema/1-0-0",
    properties: {
      page: "product_detail",
      duration_ms: 7300,
      stay_id: "stay-100",
      exit_reason: "add_to_cart"
    }
  });

  await analytics.track({
    name: "add_to_cart",
    schema: "iglu:io.openeventflow/add_to_cart/jsonschema/1-0-0",
    properties: {
      product_id: "sku-100",
      sku_id: "sku-100-red",
      quantity: 1,
      price: 199.0,
      currency: "CNY"
    }
  });
}

module.exports = {
  runEcommerceJourney
};
