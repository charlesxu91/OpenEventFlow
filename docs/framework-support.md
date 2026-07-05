# Framework Support

OpenEventFlow is organized as a monorepo so each app framework can share the same event contract while using idiomatic local APIs.

## Package Matrix

| Framework | Package | Status | Primary API |
| --- | --- | --- | --- |
| Web | `@openeventflow/web` | Implemented runtime entry | `createWebAnalytics` |
| React | `@openeventflow/react` | Implemented bindings | `OpenEventFlowProvider`, `useAnalytics`, `useScreen`, `useExposure` |
| React Native | `@openeventflow/react-native` | Implemented runtime entry | `createReactNativeAnalytics` |
| Flutter | `sdks/flutter` | Implemented Dart primitives | `OpenEventFlowClient` |
| Android | `sdks/android` | Implemented in-memory client | `OpenEventFlowClient` |
| iOS | `sdks/ios` | Implemented in-memory client | `OpenEventFlowClient` |

## Shared Contract

Every framework uses the same generated artifacts:

- JSON Schema for Snowplow/Iglu
- TypeScript interfaces for Web, React, and React Native
- Dart classes for Flutter
- Kotlin data classes for Android
- Swift structs for iOS

## Runtime Responsibilities

All framework SDKs should provide:

- `track`
- `screen`
- `identify`
- `setConsent`
- `flush`
- local queueing
- Snowplow-compatible event payloads
- PII filtering before upload

Framework-specific autotracking is optional and conservative by default.
