package io.openeventflow.recommendation;

import java.io.Serializable;

@FunctionalInterface
public interface FeatureSink extends Serializable {
  void write(InterestAggregateFunction.InterestProfile profile) throws Exception;
}
