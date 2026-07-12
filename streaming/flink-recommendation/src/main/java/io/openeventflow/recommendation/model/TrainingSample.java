package io.openeventflow.recommendation.model;

import java.io.Serializable;

public record TrainingSample(String sampleId, String userId, String impressionId, String itemType,
                             String itemId, String action, int label, long eventTime,
                             String correctsSampleId) implements Serializable {}
