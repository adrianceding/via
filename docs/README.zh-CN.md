# Via

[English](../README.md) | [中文](README.zh-CN.md)

[![CI](https://github.com/adrianceding/via/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/adrianceding/via/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](../LICENSE)

Via 是一个面向 Linux 的多线路 TCP 中继。它通过 SOCKS5 接收应用流量，并同时利用有线、Wi-Fi、蜂窝网络等多个出口；一条线路变慢或断开时，仍可通过其他线路继续传输。

> [!IMPORTANT]
> Via 的代码由 AI 编写。项目有较完整的自动化测试，也做过人工功能测试，但代码尚未经过人工审查。测试通过不代表实现一定可靠；用于重要业务或生产环境之前，请自行审查代码并评估风险。

> [!WARNING]
> Via 客户端与服务端之间目前通过明文 TCP 传输，不加密数据、不验证服务端身份，也不能保证传输内容未被篡改。请只在可信网络或受控 VPN 中使用，不要把 Via 当作加密隧道直接暴露在公网。

## 功能

- 自动发现并使用多张网卡，网卡可以按名称筛选，也可以动态增减。
- 每张合格网卡可维持 1 到 64 条长期 TCP lane，在不增加 Flow attachment 或目标 TCP 连接的前提下聚合带宽。
- 支持自适应和冗余两种传输方式，线路故障后可恢复已有连接。自适应 `fastest` 根据实测时延、DATA 容量、共享负载和切换迟滞选择线路；`distributed` 根据实测容量和负载聚合合格线路。
- SOCKS5 用户认证可以选择是否启用；客户端通过 PSK 向服务端证明身份。
- 内置只读 Web Manager，可查看线路、会话、流量和连接状态。
- Web Manager 支持中英文切换，并记住语言选择。

当前版本仅支持 TCP 和 SOCKS5 `CONNECT`，不是 VPN，也不支持 UDP。

## Docker 部署

建议使用仓库提供的 [compose.yml](../compose.yml)，在两台 Linux 主机上分别运行服务端和客户端。先在两台主机上下载仓库：

```sh
git clone https://github.com/adrianceding/via.git
cd via
```

在服务端主机上复制配置：

```sh
cp examples/config/server.yml server.yml
```

在客户端主机上复制配置：

```sh
cp examples/config/client.yml client.yml
```

运行 `openssl rand -base64 32` 生成新密钥，把它同时填入服务端的 `principals[].psk` 和客户端的 `psk`，然后将客户端的 `transport.address` 改为服务端地址。容器默认使用 UID/GID `1000:1000`；如需调整，请设置 `VIA_UID` 和 `VIA_GID`。

在服务端主机上启动：

```sh
docker compose pull server
docker compose up -d server
```

在客户端主机上启动：

```sh
docker compose pull client
docker compose up -d client
```

应用可通过客户端的 `127.0.0.1:1080` SOCKS5 代理访问网络：

```sh
curl --socks5-hostname 127.0.0.1:1080 https://example.com/
```

## 配置

Via 的配置文件使用 YAML 格式，未知字段或格式不符都会导致启动失败。发送方式、网卡筛选、SOCKS5 认证、状态页面和资源限制等选项，可查看[服务端示例](../examples/config/server.yml)和[客户端示例](../examples/config/client.yml)中的双语注释。

## 只读 Manager

在客户端或服务端启用 `status` 后，用浏览器打开 `status.listen` 中设置的地址即可进入 Manager。Manager 和 JSON API 只能查看状态，不能修改配置。

```yaml
status:
	enabled: true
	listen: "127.0.0.1:9090"
	basic_auth:
		username: "observer"
		password: "replace-with-a-strong-password"
```

设置 `basic_auth` 后，页面、静态资源、JSON API 和健康检查都需要凭据。普通 HTTP 上的 Basic Auth 不会加密凭据。

会话状态把 Probe RTT 和停滞与 DATA 容量、排队及写入中负载分开显示。容量样本经过由 RTT 推导的有界时间后会过期；过期值仍保留用于诊断，但线路放置会回退到默认容量。Manager 最多保留 120 个吞吐样本，最多显示 12 条单会话趋势，聚合趋势仍包含全部活动会话。

## 安全边界

- Via 客户端与服务端之间的连接不加密。SOCKS5 用户认证和通过 HTTP 使用的 Basic Auth 凭据也不会被加密。
- 请在可信网络或受控 VPN 中使用，并通过防火墙限制传输端口和 Manager 的访问来源。
- 示例中的密钥和密码只用于本机试用，实际部署前必须更换。

## 开发与验证

开发环境需要 Go 1.25 或更高版本，以及 Node.js 22 和 npm。真实网络测试只能在 Linux 上运行。

```sh
make production
make test-network
```

`make production` 会检查格式、运行前后端测试和竞态检测，并完成构建。`make test-network` 使用 Linux `netns`、`veth` 和 `tc netem` 验证真实的多线路故障。

## 许可证

Via 使用 [MIT License](../LICENSE)。