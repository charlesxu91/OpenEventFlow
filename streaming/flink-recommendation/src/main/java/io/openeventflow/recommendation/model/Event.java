package io.openeventflow.recommendation.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Event(
    @JsonProperty("event_id") @JsonAlias("eventId") String eventId,
    @JsonProperty("user_id") @JsonAlias("userId") String userId,
    @JsonProperty("event_name") @JsonAlias({"eventType", "event_type"}) String eventType,
    @JsonProperty("impression_id") @JsonAlias("impressionId") String impressionId,
    @JsonProperty("item_type") @JsonAlias("itemType") String itemType,
    @JsonProperty("item_id") @JsonAlias("itemId") String itemId,
    @JsonProperty("conversion_event_id") @JsonAlias("conversionEventId") String conversionEventId,
    long timestamp,
    String category,
    String brand,
    @JsonProperty("price_bucket") @JsonAlias("priceBucket") String priceBucket,
    @JsonProperty("content_type") @JsonAlias("contentType") String contentType,
    @JsonProperty("order_id") @JsonAlias("orderId") String orderId,
    @JsonProperty("payment_id") @JsonAlias("paymentId") String paymentId)
    implements Serializable {

  public static Event impression(String id, String user, String impression, String itemType, String itemId, long at) {
    return base(id, user, "recommendation_impression", impression, itemType, itemId, null, at, null, null);
  }

  public static Event action(String id, String user, String type, String impression, String itemType, String itemId, long at) {
    return base(id, user, type, impression, itemType, itemId, null, at, null, null);
  }

  public static Event payment(String id, String user, String impression, String itemType, String itemId,
                              String orderId, String paymentId, long at) {
    return base(id, user, "recommendation_payment", impression, itemType, itemId, null, at, orderId, paymentId);
  }

  public static Event refund(String id, String user, String impression, String itemType, String itemId,
                             String conversionId, long at) {
    return base(id, user, "recommendation_refund", impression, itemType, itemId, conversionId, at, null, null);
  }

  public static Event refundByPayment(String id, String user, String impression, String itemType, String itemId,
                                      String paymentId, long at) {
    return base(id, user, "recommendation_refund", impression, itemType, itemId, null, at, null, paymentId);
  }

  public static Event interest(String id, String user, String action, String category, String brand,
                               String price, String content, long at) {
    return new Event(id, user, action, null, null, null, null, at, category, brand, price, content, null, null);
  }

  private static Event base(String id, String user, String type, String impression, String itemType,
                            String itemId, String conversionId, long at, String orderId, String paymentId) {
    return new Event(id, user, type, impression, itemType, itemId, conversionId, at,
      null, null, null, null, orderId, paymentId);
  }

  public String correlationKey() {
    return impressionId + "|" + itemType + "|" + itemId;
  }

  public boolean isImpression() {
    return "impression".equals(eventType) || "recommendation_impression".equals(eventType);
  }

  public boolean isRefund() {
    return "refund".equals(eventType) || "recommendation_refund".equals(eventType);
  }
}
