# FieldLedger 农情观测台账

这是一个可独立运行的本地业务项目。核心模块保留数据接收、领域规则、持久化记录及查询接口；演练数据均为合成资料，不需要真实账号或外部业务服务。

## 运行环境

Node.js 22，核心业务不依赖第三方包。

Dockerfile 固定运行时版本。首次准备执行 docker build -t local-product .，之后以 docker run --rm --network none local-product 运行完整检查；所有 HTTP 演练只在容器内部回环地址通信。

## 开发与运行

业务测试：
    npm test
编译或语法检查：
    npm run build
现有业务演示：
    npm run acceptance
输入端演练检查：
    node demo_support/source.mjs --check
事件流订阅端到端演练（半字符断开、重发、坏事件、冲突、慢写入、重启核对）：
    node demo_support/stream_demo.mjs
合并运行：
    sh demo_support/verify.sh

输入协议、样本与故障开关见 demo_support/协议.md。输入端检查只证明本地来源的约定行为；业务演示展示当前已有功能。调用者可以在独立临时数据目录重复运行，原始样本不会被覆盖。

## 持续事件流订阅

订阅入口把网关的 text/event-stream 边接收边送入现有观测台账，核心模块为 src/sse.js（字节级 SSE 解析）、src/streams.js（订阅与流死信领域规则）、src/streamManager.js（连接生命周期）。

- 字节级解析：网络块可在任意位置切开 UTF-8 字符、字段行或空行边界，解析器以流式 TextDecoder 重组，支持 CRLF/LF、注释心跳与多行 data；单事件大小受限，内存有界。
- 位置即落盘：最后确认的不透明事件 ID 与台账写入在同一 journal 事务提交，绝不先确认位置再丢数据；未遇空行边界的半事件不确认，重连带 Last-Event-ID。
- 两套版本：不透明事件 ID 只管传输去重，观测 revision 才是业务版本。重发同正文幂等跳过；同 ID 换正文保留冲突证据并拒收。
- 坏事件隔离：传输/解码/冲突类坏事件保留原文与原因进入流死信，可单独 retry/replace/discard；业务规则拒收仍走原有 quarantine，迟到修订、删除、台账隔离规则不变。
- 值班控制：支持暂停、恢复、停止、重新配置与查看最后落盘位置；代际机制保证暂停或重配置后旧连接的积压回调不能再推进订阅。

CLI（一次性命令只持久化配置，连接循环由 stream-run 或 serve 承载）：
    node src/cli.js stream-register --root DIR --input subscription.json
    node src/cli.js stream-run      --root DIR --input '{"id":"...","settleMs":1500}'
    node src/cli.js stream-pause    --root DIR --id sub1
    node src/cli.js stream-resume   --root DIR --id sub1
    node src/cli.js stream-status   --root DIR --id sub1
    node src/cli.js stream-deadletters --root DIR --id sub1
长驻服务以 node src/cli.js serve --root DIR --manager true 启动，HTTP 接口见 /streams、/stream/pause、/stream/resume、/stream/status、/stream/runtime、/stream/deadletters。
