# 本次验证记录

日期：2026-09-05。

## 已完成

- 使用 Linux amd64、Go 1.27.1 编译 `cmd/marketd` 和 `cmd/subscriber`，成功链接原生 libzmq。
- `go vet ./...` 通过。
- `MARKETPIPE_SKIP_ZMQ_INTEGRATION=1 go test -race -v ./...`：8 个测试通过；1 个 ZeroMQ 跨进程互通测试明确跳过。
- 通过项：报价/成交与组合消息解析、超过 JavaScript 安全整数范围的 ID 保留、主动买卖方向、错误消息拒绝、分片和端点路由、配置校验、入口溢出后的序号缺口、8 个并发生产者的同 topic 次序、静默 WebSocket 超时重连、取消后的连接退出、Pong 保活和到期换连接。
- 上游 WebSocket 测试使用本机模拟服务，不依赖币安或公网。

## 未完成与原因

- 原生 ZeroMQ TCP 互通测试在此环境的网络接口查询处出现 `Operation not permitted (src/ip_resolver.cpp:542)`，libzmq 随后 abort。
- 将测试切换到本机 IPC 后，绑定也返回 `operation not permitted`。因此不能宣称 Python 与 Go 端到端互通已通过，也不能用核心测试通过来替代这个结果。
- 本次成功的最终测试命令显式设置 `MARKETPIPE_SKIP_ZMQ_INTEGRATION=1`。默认运行不会设置此变量；正常环境安装 pyzmq 后将执行 TCP 互通测试。
- 未运行币安实盘连接、Docker 镜像构建、远程 CI、macOS/Windows 实机测试或 Node.js 示例；未做持续负载/吞吐/延迟基准，不提供性能数字或生产 SLA。
- Go race 检查覆盖 Go 内存访问，不覆盖 libzmq 的 C/C++ 内部；代码以专用 goroutine + LockOSThread 保证 socket 的单线程所有权。

## 正常环境验收命令

```bash
python -m pip install -r examples/requirements.txt
go test -race -v ./...
go vet ./...
docker compose up --build -d
python examples/subscriber.py
```

验收时确认互通测试没有 SKIP、收到 quote 和 agg_trade、价格数量与源 ID 精度正确，并检查 `/readyz`、`/status`、`/metrics`。需要用实际部署网络验证断网恢复、长期运行和慢消费者的行为。
