package io.openeventflow.recommendation.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import java.io.Serializable;
import java.util.Map;

/**
 * Output model shared by the event-level and aggregate attribution jobs.
 *
 * <p>The short constructor represents a generic item/action training event. The aggregate constructor represents
 * an upsert keyed by the full recommendation attribution identity. Fields that do not apply to a representation are
 * left at their Java default value.</p>
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class TrainingSample implements Serializable {
  private final String sampleId;
  private final String userId;
  private final String impressionId;
  private final String itemType;
  private final String itemId;
  private final String action;
  private final int label;
  private final long eventTime;
  private final String correctsSampleId;

  private final AttributionKey key;
  private final long impressionTimeMillis;
  private final int position;
  private final String modelVersion;
  private final String featureSetVersion;
  private final Map<String, String> impressionFeatures;
  private final boolean clicked;
  private final boolean favorited;
  private final boolean addedToCart;
  private final boolean paid;
  private final boolean cancelled;
  private final boolean refunded;
  private final long netGmvMicros;
  private final boolean finalized;

  public TrainingSample(String sampleId, String userId, String impressionId, String itemType,
                        String itemId, String action, int label, long eventTime, String correctsSampleId) {
    this.sampleId = sampleId;
    this.userId = userId;
    this.impressionId = impressionId;
    this.itemType = itemType;
    this.itemId = itemId;
    this.action = action;
    this.label = label;
    this.eventTime = eventTime;
    this.correctsSampleId = correctsSampleId;
    this.key = null;
    this.impressionTimeMillis = 0;
    this.position = 0;
    this.modelVersion = null;
    this.featureSetVersion = null;
    this.impressionFeatures = Map.of();
    this.clicked = false;
    this.favorited = false;
    this.addedToCart = false;
    this.paid = false;
    this.cancelled = false;
    this.refunded = false;
    this.netGmvMicros = 0;
    this.finalized = false;
  }

  public TrainingSample(AttributionKey key, String userId, long impressionTimeMillis, int position,
                        String modelVersion, String featureSetVersion, Map<String, String> impressionFeatures,
                        boolean clicked, boolean favorited, boolean addedToCart, boolean paid, boolean cancelled,
                        boolean refunded, long netGmvMicros, boolean finalized) {
    this.sampleId = null;
    this.userId = userId;
    this.impressionId = null;
    this.itemType = null;
    this.itemId = null;
    this.action = null;
    this.label = 0;
    this.eventTime = 0;
    this.correctsSampleId = null;
    this.key = key;
    this.impressionTimeMillis = impressionTimeMillis;
    this.position = position;
    this.modelVersion = modelVersion;
    this.featureSetVersion = featureSetVersion;
    this.impressionFeatures = Map.copyOf(impressionFeatures);
    this.clicked = clicked;
    this.favorited = favorited;
    this.addedToCart = addedToCart;
    this.paid = paid;
    this.cancelled = cancelled;
    this.refunded = refunded;
    this.netGmvMicros = netGmvMicros;
    this.finalized = finalized;
  }

  public String sampleId() { return sampleId; }
  public String userId() { return userId; }
  public String impressionId() { return impressionId; }
  public String itemType() { return itemType; }
  public String itemId() { return itemId; }
  public String action() { return action; }
  public int label() { return label; }
  public long eventTime() { return eventTime; }
  public String correctsSampleId() { return correctsSampleId; }
  public AttributionKey key() { return key; }
  public long impressionTimeMillis() { return impressionTimeMillis; }
  public int position() { return position; }
  public String modelVersion() { return modelVersion; }
  public String featureSetVersion() { return featureSetVersion; }
  public Map<String, String> impressionFeatures() { return impressionFeatures; }
  public boolean clicked() { return clicked; }
  public boolean favorited() { return favorited; }
  public boolean addedToCart() { return addedToCart; }
  public boolean paid() { return paid; }
  public boolean cancelled() { return cancelled; }
  public boolean refunded() { return refunded; }
  public long netGmvMicros() { return netGmvMicros; }
  public boolean finalized() { return finalized; }
}
