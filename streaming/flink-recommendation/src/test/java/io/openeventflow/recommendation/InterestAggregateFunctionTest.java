package io.openeventflow.recommendation;

import io.openeventflow.recommendation.model.Event;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class InterestAggregateFunctionTest {
  @Test void decaysSignalsDeterministically() {
    var aggregate = new InterestAggregateFunction(1_000);
    var profile = aggregate.createAccumulator();
    aggregate.add(Event.interest("e1", "u1", "click", "shoes", "acme", "50-100", "product", 0), profile);
    var snapshot = aggregate.snapshot(profile, 1_000);
    assertEquals(0.5, snapshot.categories().get("shoes"), 1e-9);
    assertEquals(0.5, snapshot.brands().get("acme"), 1e-9);
    assertEquals(0.5, snapshot.priceBuckets().get("50-100"), 1e-9);
    assertEquals(0.5, snapshot.contentTypes().get("product"), 1e-9);
    assertEquals(0.5, snapshot.actions().get("click"), 1e-9);
  }

  @Test void mergePreservesEventTimeDecay() {
    var aggregate = new InterestAggregateFunction(1_000);
    var first = aggregate.createAccumulator();
    var second = aggregate.createAccumulator();
    aggregate.add(Event.interest("e1", "u1", "click", "shoes", null, null, null, 0), first);
    aggregate.add(Event.interest("e2", "u1", "click", "shoes", null, null, null, 1_000), second);
    var profile = aggregate.snapshot(aggregate.merge(first, second), 1_000);
    assertEquals(1.5, profile.categories().get("shoes"), 1e-9);
  }
}
