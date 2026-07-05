async function runShortVideoFeedJourney(analytics) {
  analytics.identify("video-user-42", { creator_segment: "high_intent" });

  await analytics.track({
    name: "video_exposed",
    schema: "iglu:io.openeventflow/video_exposed/jsonschema/1-0-0",
    properties: {
      video_id: "video-100",
      author_id: "author-7",
      page: "video_feed",
      position: 3,
      exposure_id: "vexp-100",
      visible_ratio: 0.96,
      duration_ms: 1600,
      recommend_trace_id: "vrec-abc"
    }
  });

  await analytics.track({
    name: "video_played",
    schema: "iglu:io.openeventflow/video_played/jsonschema/1-0-0",
    properties: {
      video_id: "video-100",
      author_id: "author-7",
      page: "video_feed",
      position: 3,
      play_id: "play-100",
      autoplay: true,
      network_type: "wifi",
      recommend_trace_id: "vrec-abc"
    }
  });

  await analytics.track({
    name: "video_watch",
    schema: "iglu:io.openeventflow/video_watch/jsonschema/1-0-0",
    properties: {
      video_id: "video-100",
      author_id: "author-7",
      play_id: "play-100",
      watch_id: "watch-100",
      duration_ms: 8200,
      play_duration_ms: 10000,
      completion_rate: 0.82,
      completed: false,
      exit_reason: "swipe_next"
    }
  });

  await analytics.track({
    name: "video_engaged",
    schema: "iglu:io.openeventflow/video_engaged/jsonschema/1-0-0",
    properties: {
      video_id: "video-100",
      author_id: "author-7",
      play_id: "play-100",
      engagement_id: "vlike-100",
      action: "like"
    }
  });
}

module.exports = {
  runShortVideoFeedJourney
};
