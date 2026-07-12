package io.openeventflow.recommendation;

import io.openeventflow.recommendation.model.Event;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class EventJsonProcessFunction extends ProcessFunction<String, Event> {
  public static final OutputTag<String> PARSE_ERRORS = new OutputTag<>("parse-errors") {};

  @Override
  public void processElement(String json, Context context, Collector<Event> out) {
    try {
      out.collect(EventJsonParser.parse(json));
    } catch (Exception error) {
      context.output(PARSE_ERRORS, json);
    }
  }
}
