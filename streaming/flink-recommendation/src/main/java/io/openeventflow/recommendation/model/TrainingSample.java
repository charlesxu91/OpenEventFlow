package io.openeventflow.recommendation.model;

import java.io.Serializable;
import java.util.Map;

public record TrainingSample(
    AttributionKey key,
    String userId,
    long impressionTimeMillis,
    int position,
    String modelVersion,
    String featureSetVersion,
    Map<String, String> impressionFeatures,
    boolean clicked,
    boolean favorited,
    boolean addedToCart,
    boolean paid,
    boolean cancelled,
    boolean refunded,
    long netGmvMicros,
    boolean finalized) implements Serializable {}
