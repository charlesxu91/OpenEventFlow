function compareTrackingPlans(baseline, candidate) {
  const changes = [];

  compareIdentity(changes, "namespace", "NAMESPACE_CHANGED", baseline.namespace, candidate.namespace);
  compareIdentity(changes, "schemaVendor", "SCHEMA_VENDOR_CHANGED", baseline.schemaVendor, candidate.schemaVendor);

  const candidateEvents = indexByName(candidate.events || []);
  const baselineEvents = indexByName(baseline.events || []);

  for (const baselineEvent of baseline.events || []) {
    const candidateEvent = candidateEvents.get(baselineEvent.name);
    const eventPath = `events.${baselineEvent.name}`;
    if (!candidateEvent) {
      changes.push(change("breaking", "EVENT_REMOVED", eventPath, baselineEvent, null));
      continue;
    }

    if (baselineEvent.version !== candidateEvent.version) {
      changes.push(change(
        "breaking",
        "EVENT_VERSION_CHANGED",
        `${eventPath}.version`,
        baselineEvent.version,
        candidateEvent.version
      ));
    }
    addDeprecationChange(changes, "EVENT_DEPRECATED", eventPath, baselineEvent, candidateEvent);
    compareFields(changes, eventPath, baselineEvent, candidateEvent);
  }

  for (const candidateEvent of candidate.events || []) {
    if (!baselineEvents.has(candidateEvent.name)) {
      changes.push(change("compatible", "EVENT_ADDED", `events.${candidateEvent.name}`, null, candidateEvent));
    }
  }

  return {
    compatible: !changes.some(({ severity }) => severity === "breaking"),
    changes
  };
}

function compareIdentity(changes, path, code, before, after) {
  if (before !== after) {
    changes.push(change("breaking", code, path, before, after));
  }
}

function compareFields(changes, eventPath, baselineEvent, candidateEvent) {
  const baselineProperties = baselineEvent.properties || {};
  const candidateProperties = candidateEvent.properties || {};
  const baselineRequired = new Set(baselineEvent.required || []);
  const candidateRequired = new Set(candidateEvent.required || []);

  for (const [fieldName, baselineField] of Object.entries(baselineProperties)) {
    const candidateField = candidateProperties[fieldName];
    const fieldPath = `${eventPath}.properties.${fieldName}`;
    if (!candidateField) {
      changes.push(change("breaking", "FIELD_REMOVED", fieldPath, baselineField, null));
      continue;
    }
    if (baselineField.type !== candidateField.type) {
      changes.push(change(
        "breaking",
        "FIELD_TYPE_CHANGED",
        `${fieldPath}.type`,
        baselineField.type,
        candidateField.type
      ));
    }
    if (!baselineRequired.has(fieldName) && candidateRequired.has(fieldName)) {
      changes.push(change(
        "breaking",
        "FIELD_MADE_REQUIRED",
        fieldPath,
        baselineField,
        candidateField
      ));
    } else if (baselineRequired.has(fieldName) && !candidateRequired.has(fieldName)) {
      changes.push(change("compatible", "FIELD_MADE_OPTIONAL", fieldPath, baselineField, candidateField));
    }
    addDeprecationChange(changes, "FIELD_DEPRECATED", fieldPath, baselineField, candidateField);
  }

  for (const [fieldName, candidateField] of Object.entries(candidateProperties)) {
    if (Object.hasOwn(baselineProperties, fieldName)) {
      continue;
    }
    const required = candidateRequired.has(fieldName);
    const severity = required ? "breaking" : "compatible";
    const code = required ? "FIELD_ADDED_REQUIRED" : "FIELD_ADDED_OPTIONAL";
    changes.push(change(severity, code, `${eventPath}.properties.${fieldName}`, null, candidateField));
  }
}

function addDeprecationChange(changes, code, path, baselineValue, candidateValue) {
  const before = deprecationMetadata(baselineValue);
  const after = deprecationMetadata(candidateValue);
  if (before == null && after != null) {
    changes.push(change("deprecated", code, path, null, after));
  }
}

function deprecationMetadata(value) {
  if (!value || !Object.hasOwn(value, "deprecated") || value.deprecated === false) {
    return null;
  }
  return value.deprecated;
}

function indexByName(values) {
  return new Map(values.map((value) => [value.name, value]));
}

function change(severity, code, path, before, after) {
  return { severity, code, path, before, after };
}

module.exports = { compareTrackingPlans };
