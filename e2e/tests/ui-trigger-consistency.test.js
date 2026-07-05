const assert = require("node:assert/strict");
const test = require("node:test");
const { runUiDrivenEcommerceJourney } = require("../src/ui-model");

test("UI model triggers exposure, click, stay, and cart events consistently", async () => {
  const result = await runUiDrivenEcommerceJourney();

  assert.deepEqual(result.counts, {
    exposures: 1,
    clicks: 1,
    stays: 1,
    carts: 1,
    badEvents: 0
  });
  assert.equal(result.snapshot.fact_product_exposures[0].product_id, "sku-ui-100");
  assert.equal(result.snapshot.fact_product_clicks[0].click_id, "clk-ui-100");
  assert.equal(result.snapshot.fact_page_stays[0].duration_ms, 8200);
  assert.equal(result.snapshot.fact_cart_adds[0].sku_id, "sku-ui-100-blue");
});
