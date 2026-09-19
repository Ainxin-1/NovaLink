# NovaLink

免费、轻量、简单易用的 VPN 客户端（Windows / Android）。

成熟开源核心（sing-box）负责底层网络，NovaLink 负责：

- NovaLink Node Pipeline：多来源免费节点 获取 → 解析 → 去重 → 深度检测 → 节点池 → 发布
- NovaLink Client：节点列表、设备端协议级实测、一键连接/断开、国内直连分流、系统代理接管、自动换节点

## 公开免费池的真实水平（2026-09-19 国内家宽实测）

写在前头，避免按"VPN 客户端"的期待去用它：

| 项目 | 实测值 |
|---|---|
| 全量来源去重候选 | 44,841 |
| TCP 可达 | 16,439（36%） |
| **协议级真通外网**（经节点取回 google 204） | **约 19 个 / 抽样 4,000，≈0.05%** |
| 同时可维持的可用节点 | 约 11 个（复测会衰减，免费节点以小时计失效） |
| 最优节点延迟（内核 delay 口径） | 约 450ms |
| 冷启动页面级请求 | google 1.0~2.0s，youtube 1.3~2.3s |

三条结论，都是量出来的：**① 各协议、Cloudflare 与非 CF IP 段之间通过率没有显著差别
（0.05% vs 0.06%），不存在"挑对协议就能稳定"的结构性过滤**；**② 扩大来源只是线性加候选，
不提高命中率**（新增 3 个高星源：2,068 个 TCP 存活 → 7 通，0.34%）；**③ 加大扫描广度也无收益**
（扫全池 3,000 个耗时 8m58s 与只扫已发布 800 个耗时 1m52s，可用节点同样都是 11 个）。

所以：需要"稳定 + 低延迟 + 大流量"的场景，请自备节点或自建，本项目的免费池做不到；
它的价值在于**不骗人**——只把本机实测确认能用的节点交给你，且国内站点不进隧道。

## 目录

```
node/     节点管线（Go，novanode）
client/   Windows 客户端（Go + 内嵌 Web UI，novalink）
core/     VPN 核心（sing-box，运行时下载）
docs/     预研报告与各阶段验证记录
verify/   阶段验证脚本与产物
```

## 节点管线用法

```
cd node && go build -o novanode .
./novanode fetch -dir data    # 获取→解析→去重→深度检测→发布
./novanode status -dir data   # 查看节点池统计
```

发布产物在 `data/published/`：通用 base64 订阅 + sing-box JSON + `pool_slim.json`
（客户端下载用的精简池，3,000 节点上限约 2MB；全量 `data/pool.json` 已达 15,000 节点 / 17MB，
只作为管线状态，不给客户端整份拉取）。

## 客户端用法

```
cd client && go build -o novalink .
./novalink.exe -dir data      # 打开 http://127.0.0.1:7892
```

连接成功后自动接管系统代理，断开自动恢复原状；
节点失效自动更换（最多连续尝试 6 个候选）。

## 持续更新

`.github/workflows/node-pipeline.yml` 每 2 小时运行一次管线并提交发布产物，
提交后即可通过 jsDelivr 镜像访问：

```
https://cdn.jsdelivr.net/gh/<owner>/<repo>@main/data/published/nodes_base64.txt
```

## 免责声明

免费节点不等于可信节点，不保证可用性、速度与隐私安全。
请遵守所在地区法律法规。

## 许可证

第三方组件及许可证见 THIRD_PARTY_LICENSES.txt。
