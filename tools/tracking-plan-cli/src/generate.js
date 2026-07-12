const fs = require("node:fs");
const path = require("node:path");

const TYPE_MAP = {
  string: { json: "string", kotlin: "String", swift: "String", typescript: "string", dart: "String" },
  integer: { json: "integer", kotlin: "Long", swift: "Int", typescript: "number", dart: "int" },
  number: { json: "number", kotlin: "Double", swift: "Double", typescript: "number", dart: "double" },
  boolean: { json: "boolean", kotlin: "Boolean", swift: "Bool", typescript: "boolean", dart: "bool" },
  object: {
    json: "object",
    kotlin: "Map<String, Any?>",
    swift: "[String: AnyCodable]",
    typescript: "Record<string, unknown>",
    dart: "Map<String, Object?>"
  }
};

function generateArtifacts(trackingPlan, outputDir) {
  validateTrackingPlan(trackingPlan);

  const generated = [];
  const events = trackingPlan.events;

  for (const event of events) {
    const schemaRelativePath = path.join(
      "schemas",
      trackingPlan.schemaVendor,
      event.name,
      "jsonschema",
      `${event.version}.json`
    );
    const schemaPath = path.join(outputDir, schemaRelativePath);
    writeJson(schemaPath, buildJsonSchema(trackingPlan, event));
    generated.push(toPortablePath(schemaRelativePath));
  }

  const kotlinRelativePath = path.join("kotlin", "OpenEventFlowEvents.kt");
  writeText(path.join(outputDir, kotlinRelativePath), buildKotlin(events, trackingPlan.schemaVendor));
  generated.push(toPortablePath(kotlinRelativePath));

  const swiftRelativePath = path.join("swift", "OpenEventFlowEvents.swift");
  writeText(path.join(outputDir, swiftRelativePath), buildSwift(events, trackingPlan.schemaVendor));
  generated.push(toPortablePath(swiftRelativePath));

  const typescriptRelativePath = path.join("typescript", "openeventflow-events.ts");
  writeText(path.join(outputDir, typescriptRelativePath), buildTypeScript(events, trackingPlan.schemaVendor));
  generated.push(toPortablePath(typescriptRelativePath));

  const dartRelativePath = path.join("dart", "open_event_flow_events.dart");
  writeText(path.join(outputDir, dartRelativePath), buildDart(events, trackingPlan.schemaVendor));
  generated.push(toPortablePath(dartRelativePath));

  return { generated };
}

function validateTrackingPlan(plan) {
  if (!plan || typeof plan !== "object") {
    throw new Error("tracking plan must be an object");
  }
  if (!plan.schemaVendor) {
    throw new Error("schemaVendor is required");
  }
  if (!plan.namespace) {
    throw new Error("namespace is required");
  }
  if (!Array.isArray(plan.events) || plan.events.length === 0) {
    throw new Error("events must contain at least one event");
  }

  const names = new Set();
  for (const event of plan.events) {
    if (!event.name) {
      throw new Error("event.name is required");
    }
    if (names.has(event.name)) {
      throw new Error(`duplicate event name: ${event.name}`);
    }
    names.add(event.name);
    if (!event.version) {
      throw new Error(`event.version is required for ${event.name}`);
    }
    validateDeprecation(event.deprecated, `event ${event.name}`);
    if (!event.properties || typeof event.properties !== "object") {
      throw new Error(`event.properties is required for ${event.name}`);
    }
    const required = new Set(event.required || []);
    for (const requiredField of required) {
      if (!event.properties[requiredField]) {
        throw new Error(`required field ${requiredField} is not defined for ${event.name}`);
      }
    }
    for (const [fieldName, field] of Object.entries(event.properties)) {
      if (!field.type || !TYPE_MAP[field.type]) {
        throw new Error(`unsupported type for ${event.name}.${fieldName}: ${field.type}`);
      }
      validateDeprecation(field.deprecated, `field ${event.name}.${fieldName}`);
    }
  }
}

function validateDeprecation(deprecated, subject) {
  if (deprecated === undefined || deprecated === false) {
    return;
  }
  if (!deprecated || typeof deprecated !== "object" || Array.isArray(deprecated)) {
    throw new Error(`${subject} deprecated must be an object`);
  }
  const allowedKeys = new Set(["since", "replacement", "removeAfter"]);
  for (const key of Object.keys(deprecated)) {
    if (!allowedKeys.has(key)) {
      throw new Error(`${subject} deprecated has unsupported key ${key}`);
    }
  }
  for (const key of ["since", "replacement", "removeAfter"]) {
    if (typeof deprecated[key] !== "string" || deprecated[key].trim() === "") {
      throw new Error(`${subject} deprecated.${key} is required`);
    }
  }
  const since = parseIsoDate(deprecated.since);
  if (since == null) {
    throw new Error(`${subject} deprecated.since must be a valid YYYY-MM-DD date`);
  }
  const removeAfter = parseIsoDate(deprecated.removeAfter);
  if (removeAfter == null) {
    throw new Error(`${subject} deprecated.removeAfter must be a valid YYYY-MM-DD date`);
  }
  const ninetyDaysMs = 90 * 24 * 60 * 60 * 1000;
  if (removeAfter - since < ninetyDaysMs) {
    throw new Error(`${subject} deprecated.removeAfter must be at least 90 days after since`);
  }
}

function parseIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }
  const timestamp = Date.parse(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(timestamp) || new Date(timestamp).toISOString().slice(0, 10) !== value) {
    return null;
  }
  return timestamp;
}

function buildJsonSchema(plan, event) {
  const properties = {};
  for (const [fieldName, field] of Object.entries(event.properties)) {
    properties[fieldName] = {
      type: TYPE_MAP[field.type].json,
      description: field.description || ""
    };
    retainDeprecation(properties[fieldName], field.deprecated);
  }

  const schema = {
    $schema: "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    self: {
      vendor: plan.schemaVendor,
      name: event.name,
      format: "jsonschema",
      version: event.version
    },
    type: "object",
    description: event.description || "",
    additionalProperties: false,
    properties,
    required: event.required || []
  };
  retainDeprecation(schema, event.deprecated);
  return schema;
}

function retainDeprecation(schemaNode, deprecated) {
  if (deprecated && typeof deprecated === "object") {
    schemaNode.deprecated = true;
    schemaNode["x-openeventflow-deprecation"] = { ...deprecated };
  }
}

function buildKotlin(events, schemaVendor) {
  const lines = [
    "package io.openeventflow.generated",
    "",
    "// Generated by @openeventflow/tracking-plan-cli. Do not edit by hand.",
    "",
    "object OpenEventFlowSchemas {"
  ];

  for (const event of events) {
    lines.push(`    const val ${toConstantName(event.name)}_SCHEMA = "iglu:${schemaVendor}/${event.name}/jsonschema/${event.version}"`);
  }

  lines.push("}", "");

  for (const event of events) {
    lines.push(`data class ${toPascalCase(event.name)}Event(`);
    const required = new Set(event.required || []);
    const properties = Object.entries(event.properties);
    properties.forEach(([fieldName, field], index) => {
      const nullable = required.has(fieldName) ? "" : "?";
      const defaultValue = required.has(fieldName) ? "" : " = null";
      const comma = index === properties.length - 1 ? "" : ",";
      lines.push(`    val ${toCamelCase(fieldName)}: ${TYPE_MAP[field.type].kotlin}${nullable}${defaultValue}${comma}`);
    });
    lines.push(")", "");
  }

  return `${lines.join("\n")}\n`;
}

function buildSwift(events, schemaVendor) {
  const lines = [
    "import Foundation",
    "",
    "// Generated by @openeventflow/tracking-plan-cli. Do not edit by hand.",
    "",
    "public enum OpenEventFlowSchemas {"
  ];

  for (const event of events) {
    lines.push(`    public static let ${toCamelCase(event.name)}Schema = "iglu:${schemaVendor}/${event.name}/jsonschema/${event.version}"`);
  }

  lines.push("}", "");

  for (const event of events) {
    lines.push(`public struct ${toPascalCase(event.name)}Event: Codable {`);
    const required = new Set(event.required || []);
    for (const [fieldName, field] of Object.entries(event.properties)) {
      const optional = required.has(fieldName) ? "" : "?";
      lines.push(`    public let ${toCamelCase(fieldName)}: ${TYPE_MAP[field.type].swift}${optional}`);
    }
    lines.push("}");
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function buildTypeScript(events, schemaVendor) {
  const lines = [
    "// Generated by @openeventflow/tracking-plan-cli. Do not edit by hand.",
    "",
    "export interface OpenEventFlowEventEnvelope<TProperties extends Record<string, unknown>> {",
    "  name: string;",
    "  schema: string;",
    "  properties: TProperties;",
    "}",
    ""
  ];

  for (const event of events) {
    const interfaceName = `${toPascalCase(event.name)}Event`;
    const required = new Set(event.required || []);
    lines.push(`export const ${toConstantName(event.name)}_SCHEMA = "iglu:${schemaVendor}/${event.name}/jsonschema/${event.version}";`);
    lines.push(`export interface ${interfaceName} {`);
    for (const [fieldName, field] of Object.entries(event.properties)) {
      const optional = required.has(fieldName) ? "" : "?";
      lines.push(`  ${toCamelCase(fieldName)}${optional}: ${TYPE_MAP[field.type].typescript};`);
    }
    lines.push("}");
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function buildDart(events, schemaVendor) {
  const lines = [
    "// Generated by @openeventflow/tracking-plan-cli. Do not edit by hand.",
    "",
    "abstract interface class OpenEventFlowGeneratedEvent {",
    "  String get name;",
    "  String get schema;",
    "  Map<String, Object?> toJson();",
    "}",
    ""
  ];

  for (const event of events) {
    const className = `${toPascalCase(event.name)}Event`;
    const required = new Set(event.required || []);
    lines.push(`class ${className} implements OpenEventFlowGeneratedEvent {`);
    lines.push(`  static const String ${toCamelCase(event.name)}Schema = "iglu:${schemaVendor}/${event.name}/jsonschema/${event.version}";`);
    lines.push("");
    for (const [fieldName, field] of Object.entries(event.properties)) {
      const nullable = required.has(fieldName) ? "" : "?";
      lines.push(`  final ${TYPE_MAP[field.type].dart}${nullable} ${toCamelCase(fieldName)};`);
    }
    lines.push("");
    lines.push(`  const ${className}({`);
    for (const [fieldName] of Object.entries(event.properties)) {
      const requiredKeyword = required.has(fieldName) ? "required " : "";
      lines.push(`    ${requiredKeyword}this.${toCamelCase(fieldName)},`);
    }
    lines.push("  });");
    lines.push("");
    lines.push("  @override");
    lines.push(`  String get name => "${event.name}";`);
    lines.push("");
    lines.push("  @override");
    lines.push(`  String get schema => ${toCamelCase(event.name)}Schema;`);
    lines.push("");
    lines.push("  @override");
    lines.push("  Map<String, Object?> toJson() => {");
    for (const [fieldName] of Object.entries(event.properties)) {
      lines.push(`        "${fieldName}": ${toCamelCase(fieldName)},`);
    }
    lines.push("      };");
    lines.push("}");
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function writeJson(filePath, value) {
  writeText(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeText(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, value);
}

function toPascalCase(value) {
  return value
    .split(/[_\-\s]+/)
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join("");
}

function toCamelCase(value) {
  const pascal = toPascalCase(value);
  return `${pascal.charAt(0).toLowerCase()}${pascal.slice(1)}`;
}

function toConstantName(value) {
  return value.replace(/[^a-zA-Z0-9]+/g, "_").replace(/([a-z])([A-Z])/g, "$1_$2").toUpperCase();
}

function toPortablePath(value) {
  return value.split(path.sep).join("/");
}

module.exports = {
  buildJsonSchema,
  buildDart,
  buildKotlin,
  buildSwift,
  buildTypeScript,
  generateArtifacts,
  validateTrackingPlan
};
