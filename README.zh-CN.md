# OpenEventFlow

[English README](README.md)

![OpenEventFlow hero](docs/assets/openeventflow-hero.png)

OpenEventFlow 是一个面向大型 App 的开源端到端事件数据流项目。它把前端和移动端埋点、事件校验、Kafka/Redpanda 流、ClickHouse 数仓建模，以及最终的数据消费连接成一条完整链路。

它提供一套 contract-first 的方式，把 Web、React、React Native、Android、iOS、Flutter 端产生的用户行为事件，稳定地采集、校验、发送到 Kafka/Redpanda，进入 ClickHouse 风格的数仓分层，并服务 BI、推荐、广告、实验、数据产品等下游消费者。

这个项目适合把曝光、点击、停留、加购、播放、观看时长、完播、点赞、分享等行为数据当成生产级数据契约来治理的团队，而不是把埋点当成随意打印的客户端日志。

## 为什么做 OpenEventFlow

很多埋点 SDK 的重点是分析报表。但大型 App 团队通常更需要一套数据基础设施：

- 事件上线前可以评审、版本化、生成类型代码
- 多端 SDK API 保持一致的事件语义
- 停留时长和观看时长需要感知前后台、暂停、恢复
- 数据入仓前需要 schema 校验和 bad event 路由
- Kafka/Redpanda 流要能服务推荐、广告、实验、风控等下游消费
- ClickHouse 数仓要有清晰的 ODS、DWD、fact、ADS 分层
- 本地 e2e 测试要能证明 UI 触发的埋点和最终数仓结果一致

OpenEventFlow 关注的是从埋点到消费的完整工程链路。它可以和 Snowplow、Segment/CDP、BI、推荐系统、实验平台、内部数仓平台一起使用。

## 设计理念

OpenEventFlow 遵循六个核心原则：

- **契约先行：** 先定义和评审 tracking plan，再生成 schema 和多端 SDK 类型。
- **SDK 是数据入口：** SDK 负责采集、标准化、排队、flush，不绑定报表产品。
- **Collector 中转：** 客户端只发到 Collector，不直接连接 Kafka、Redpanda 或 ClickHouse。
- **生命周期感知：** 停留时长、观看时长会排除后台、暂停等不应计入的时间。
- **默认可治理：** consent、identity、session、schema validation、bad event 都是一等能力。
- **面向数仓：** 事件最终进入清晰的 ODS、DWD、fact、ADS 分层，便于下游可靠消费。

## 架构

```text
Web / React / React Native / Android / iOS / Flutter app
  -> OpenEventFlow SDK
  -> OpenEventFlow Collector 或 Snowplow Collector
  -> Redpanda / Kafka topic
  -> schema 校验 / enrich / bad-event 路由
  -> 推荐归因 / 实时兴趣计算
  -> 训练样本 / 特征写入边界
  -> Warehouse loader
  -> ClickHouse ODS / DWD / fact / ADS 表
  -> BI / 推荐 / 广告 / 实验 / 数据产品
```

SDK 层让业务 App 的埋点调用保持稳定；Collector 和流式层负责接入控制；数仓层把行为事件加工成可以被下游信任的数据资产。

## 当前包含什么

- JavaScript 共享 analytics runtime：队列、consent、identity、session、flush
- Web、React、React Native SDK
- Android Kotlin、iOS Swift、Flutter/Dart SDK
- tracking-plan CLI：生成 JSON Schema、TypeScript、Kotlin、Swift、Dart 产物
- Tracking Plan 兼容性检查：区分兼容、弃用和破坏性变更
- Collector：API Key、健康与就绪检查、请求体限制、异步 broker 确认、schema 校验和 bad-event 路由
- 推荐归因核心：去重、事件时间窗口、负样本、退款修正和迟到事件分流
- Flink 推荐归因与实时兴趣作业：Kafka/Redpanda 接线、Watermark、Checkpoint、状态 TTL 和可注入特征 sink
- Snowplow self-describing event adapter
- Warehouse loader 和 ClickHouse HTTP adapter
- ClickHouse DDL 和 dbt 模型：ODS、DWD、fact、ADS 分层
- 电商 e2e 场景：曝光、点击、停留、加购
- feed 流短视频 e2e 场景：曝光、播放、观看时长、完播、点赞、分享
- Redpanda 和 ClickHouse 的 Docker Compose 模板
- Kubernetes 和 Snowplow 部署模板

## 快速开始

安装依赖：

```bash
npm install
```

从示例 tracking plan 生成 schema 和多端事件类型：

```bash
npm run codegen
```

运行标准验证：

```bash
npm run verify
```

启动真实 Redpanda 和 ClickHouse 并运行 smoke 测试：

```bash
docker compose -f deploy/docker/docker-compose.yml up -d
npm run smoke:docker
npm run smoke:docker:video
npm run smoke:dbt
```

生成产物会写入 `mobile/generated`。

## 项目目录

```text
.github/                     GitHub workflow、issue 模板、PR 模板
deploy/                      Docker Compose、Kubernetes、Snowplow 部署模板
docs/                        架构、SDK、tracking、数仓、测试文档
e2e/                         电商和短视频从 App 到数仓的 e2e 测试
examples/                    tracking plan 示例和 SDK 使用示例
mobile/generated/            生成的 JSON Schema 和多端事件类型
packages/core/               共享 analytics runtime
packages/collector/          Collector、校验、topic 发布、HTTP server
packages/recommendation/     推荐归因与兴趣计算领域逻辑
packages/snowplow-adapter/   Snowplow self-describing event adapter
packages/warehouse/          Warehouse loader 和 ClickHouse adapter
packages/web/                Browser SDK
packages/react/              React bindings
packages/react-native/       React Native bindings
sdks/android/                Android Kotlin SDK
sdks/ios/                    iOS Swift SDK
sdks/flutter/                Flutter/Dart SDK
tools/tracking-plan-cli/     tracking plan schema 和代码生成器
streaming/flink-recommendation/ Flink 推荐归因与实时兴趣作业
warehouse/dbt/               数仓分层 dbt 模型
```

更详细的目录说明见 [docs/repository-structure.md](docs/repository-structure.md)。

## 短视频事件链路示例

```text
用户看到视频卡片
  -> video_exposed
用户开始播放
  -> video_played
用户前台观看 12.5 秒
  -> video_watch
用户点赞并分享
  -> video_engaged
Collector 校验事件
  -> Redpanda topic
Warehouse loader 消费 topic
  -> fact_video_* 表
ADS 层聚合每日视频行为指标
```

电商场景也采用相同链路，例如商品曝光、点击、页面停留和加购。

## 验证范围

当前测试覆盖：

- SDK 队列、consent、identity、session、flush
- 停留时长的前后台语义
- Web 生命周期处理
- React 和 React Native bindings
- Native 和 Flutter SDK API contract
- tracking-plan 代码生成
- Tracking Plan 兼容性检查
- Collector 鉴权、探针、请求限制、broker 确认、schema 校验和 bad-event 路由
- 推荐归因、退款修正、负样本和兴趣衰减规则
- Flink 推荐模块测试（`mvn -f streaming/flink-recommendation/pom.xml test`）
- ODS、DWD、fact、ADS 数仓写入
- 电商和短视频 e2e 一致性
- 真实 Redpanda 和 ClickHouse smoke 测试

常用命令：

```bash
npm test
npm run test:mobile
npm run smoke:dbt
npm run smoke:docker
npm run smoke:docker:video
```

## 文档

- [架构](docs/architecture.md)
- [设计理念](docs/design-principles.md)
- [框架支持](docs/framework-support.md)
- [移动端工程化](docs/mobile-engineering.md)
- [Tracking Plan](docs/tracking-plan.md)
- [Schema 演进](docs/schema-evolution.md)
- [推荐数据链路](docs/recommendation-pipeline.md)
- [停留时长](docs/stay-duration.md)
- [数仓](docs/warehouse.md)
- [本地链路](docs/local-pipeline.md)
- [E2E 测试](docs/e2e-testing.md)
- [路线图](ROADMAP.md)

## 项目状态

OpenEventFlow 当前处于开源首版阶段。仓库已经包含契约生成与兼容性检查、加固后的 Collector 基础能力、推荐归因与兴趣逻辑、Flink 作业示例、数仓模型、部署模板和 e2e 验证。持久化 SDK 队列、真实生产 Kafka adapter、OpenTelemetry 看板、完整隐私治理和生产运维仍属于后续工作。

## 贡献

欢迎贡献。提交 issue 或 pull request 前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)、[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) 和 [SECURITY.md](SECURITY.md)。

## License

Apache License 2.0. 见 [LICENSE](LICENSE)。
