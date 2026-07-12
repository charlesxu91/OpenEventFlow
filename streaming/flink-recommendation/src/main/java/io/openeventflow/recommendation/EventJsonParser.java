package io.openeventflow.recommendation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openeventflow.recommendation.model.Event;

public final class EventJsonParser {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private EventJsonParser() {}

  public static Event parse(String json) throws Exception {
    Event event = MAPPER.readValue(json, Event.class);
    if (event.eventId() == null || event.eventId().isBlank()
        || event.userId() == null || event.userId().isBlank()
        || event.eventType() == null || event.eventType().isBlank()) {
      throw new IllegalArgumentException("event_id, user_id and event_name are required");
    }
    return event;
  }
}
