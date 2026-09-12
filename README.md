# NovaLink

免费、轻量、简单易用的 VPN 客户端（Windows / Android）。

成熟开源核心（sing-box）负责底层网络，NovaLink 负责：

- NovaLink Node Pipeline：多来源免费节点 获取 → 解析 → 去重 → 深度检测 → 节点池 → 发布
- NovaLink Client：节点列表、设备端检测、一键连接/断开、系统代理接管、自动换节点

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

发布产物在 `data/published/`：通用 base64 订阅 + sing-box JSON 双格式。

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
