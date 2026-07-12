package io.openeventflow.recommendation.model;

import java.io.Serializable;

public record AttributionKey(String requestId, String impressionId, String productId)
    implements Serializable {
  @Override public String toString() {
    return requestId + ':' + impressionId + ':' + productId;
  }
}
