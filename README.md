# ES-APM 分片均衡器

## 项目简介
ES-APM 分片均衡器是一个自动化工具，用于解决 Elasticsearch APM 集群中的分片分布不均问题。它通过监控和自动迁移分片来确保集群负载均衡，防止单个节点承担过多写入压力，从而提高 APM 数据上报的可靠性。

### 主要功能
- 自动检测并平衡 APM 索引的主分片分布
- 智能选择最优目标节点进行分片迁移
- 实时监控迁移进度和集群健康状态
- 支持自动重试和错误恢复
- 提供详细的操作日志和状态报告

## 系统架构

### 核心组件
1. **索引监控器**
   - 定期检查最新的 APM 索引
   - 分析分片分布情况

2. **负载均衡器**
   - 计算节点负载分数
   - 评估最优迁移方案
   - 执行分片迁移操作

3. **迁移监控器**
   - 追踪分片迁移进度
   - 监控数据同步状态
   - 确保迁移完整性

### 工作流程
1. 定期扫描最新的 APM 索引
2. 检查各节点的主分片分布
3. 当发现节点存在多个主分片时触发均衡
4. 计算最优目标节点
5. 执行分片迁移
6. 监控迁移进度直至完成

## 配置说明

### 环境变量
```env
ES_HOST=https://your-es-host:9200
ES_USER=elastic
ES_PASSWORD=your-password
```

## 部署方法

### Docker 部署

```
# 构建镜像
docker build -t es-shard-balancer:latest .

# 运行容器
docker run -d \
  --name es-shard-balancer \
  -e ES_HOST=https://your-es-host:9200 \
  -e ES_USER=elastic \
  -e ES_PASSWORD=your-password \
  --restart unless-stopped \
  es-shard-balancer:latest
```

### Kubernetes 部署

1. 创建部署文件 es-shard-balancer-deployment.yaml
```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: es-shard-balancer
  namespace: elastic
spec:
  replicas: 1
  selector:
    matchLabels:
      app: es-shard-balancer
  template:
    metadata:
      labels:
        app: es-shard-balancer
    spec:
      containers:
      - name: es-shard-balancer
        image: es-shard-balancer:latest
        imagePullPolicy: Always
        env:
        - name: ES_HOST
          value: ''
        - name: ES_USER
          value: ''
        - name: ES_PASSWORD
          value: ''
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "200m"
```

2. 应用配置

```
kubectl apply -f es-shard-balancer-deployment.yaml
```

## 项目结构
```
.
├── Dockerfile
├── README.md
├── requirements.txt
└── shard_balancer.py
```

## 注意事项
1. 确保有足够的磁盘空间用于分片迁移
2. 建议在非高峰期进行分片均衡操作
3. 监控集群健康状态，避免影响生产环境
4. 定期检查日志确保服务正常运行

## 常见问题
### Q: 为什么需要分片均衡？
A: APM 索引的写入压力较大，如果主分片过度集中在某个节点，可能导致该节点负载过高，影响 APM 数据上报的性能和可靠性。

### Q: 如何判断是否需要均衡？
A: 当发现某个节点上的主分片数量超过1个时，系统会自动触发均衡操作。

### Q: 均衡过程是否会影响服务？
A: 分片迁移过程是增量同步，不会中断服务。但建议在非高峰期进行操作以减少影响。


## 运行日志
```
ubuntu@ip-172-31-87-87:~/sungaomeng/es$ python3 balance_apm_shards.py
2024-11-21 12:05:46 - INFO - 开始执行分片平衡任务
2024-11-21 12:05:46 - INFO - 找到最新的APM索引: .ds-traces-apm-default-2024.11.21-010484
2024-11-21 12:05:46 - INFO - 索引 .ds-traces-apm-default-2024.11.21-010484 的分片数量: 3
2024-11-21 12:05:46 - INFO - 成功获取 5 个节点的状态信息
2024-11-21 12:05:46 - INFO -
当前集群状态摘要:
2024-11-21 12:05:46 - INFO - 节点 elasticsearch-master-1:
2024-11-21 12:05:46 - INFO -   - 主分片数量: 0
2024-11-21 12:05:46 - INFO -   - CPU使用率: 32%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 39%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 49.5%
2024-11-21 12:05:46 - INFO - ----------------------------------------
2024-11-21 12:05:46 - INFO - 节点 elasticsearch-master-2:
2024-11-21 12:05:46 - INFO -   - 主分片数量: 0
2024-11-21 12:05:46 - INFO -   - CPU使用率: 22%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 64%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 52.6%
2024-11-21 12:05:46 - INFO - ----------------------------------------
2024-11-21 12:05:46 - INFO - 节点 elasticsearch-master-4:
2024-11-21 12:05:46 - INFO -   - 主分片数量: 2
2024-11-21 12:05:46 - INFO -   - CPU使用率: 62%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 37%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 74.9%
2024-11-21 12:05:46 - INFO - ----------------------------------------
2024-11-21 12:05:46 - INFO - 节点 elasticsearch-master-0:
2024-11-21 12:05:46 - INFO -   - 主分片数量: 0
2024-11-21 12:05:46 - INFO -   - CPU使用率: 30%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 37%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 61.2%
2024-11-21 12:05:46 - INFO - ----------------------------------------
2024-11-21 12:05:46 - INFO - 节点 elasticsearch-master-3:
2024-11-21 12:05:46 - INFO -   - 主分片数量: 1
2024-11-21 12:05:46 - INFO -   - CPU使用率: 45%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 65%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 88.8%
2024-11-21 12:05:46 - INFO - ----------------------------------------
2024-11-21 12:05:46 - INFO -
发现节点 elasticsearch-master-4 有 2 个主分片，开始进行负载均衡...
2024-11-21 12:05:46 - INFO - 找到最佳目标节点: elasticsearch-master-1
  - CPU使用率: 32%
  - 堆内存使用率: 39%
  - 磁盘使用率: 49.5%
  - 当前主分片数: 0
  - 综合评分: 22.98
2024-11-21 12:05:46 - INFO -
================================================================================
2024-11-21 12:05:46 - INFO - 分片迁移详情:
2024-11-21 12:05:46 - INFO - 索引名称: .ds-traces-apm-default-2024.11.21-010484
2024-11-21 12:05:46 - INFO - 分片编号: 2
2024-11-21 12:05:46 - INFO - 从节点: elasticsearch-master-4
2024-11-21 12:05:46 - INFO -   - CPU使用率: 62%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 37%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 74.9%
2024-11-21 12:05:46 - INFO - 迁移到节点: elasticsearch-master-1
2024-11-21 12:05:46 - INFO -   - CPU使用率: 32%
2024-11-21 12:05:46 - INFO -   - 堆内存使用率: 39%
2024-11-21 12:05:46 - INFO -   - 磁盘使用率: 49.5%
2024-11-21 12:05:46 - INFO - 迁移原因: 源节点存在多个主分片，需要进行负载均衡
2024-11-21 12:05:46 - INFO - ================================================================================
2024-11-21 12:05:46 - INFO - 开始移动分片: 从 elasticsearch-master-4 到 elasticsearch-master-1
2024-11-21 12:05:46 - INFO - 开始监控分片迁移进度 - 索引: .ds-traces-apm-default-2024.11.21-010484, 分片: 2, 目标节点: elasticsearch-master-1
2024-11-21 12:05:46 - INFO - 源节点: elasticsearch-master-4 -> 目标节点: elasticsearch-master-1 (172.31.29.90)
阶段: index
文件迁移进度: 100.0% (0.00 B/0.00 B) 速度: 0.00 B/s
Translog进度: 100.0% (0/-1 ops)
已用时: 0.0s
2024-11-21 12:05:50 - INFO - 源节点: elasticsearch-master-4 -> 目标节点: elasticsearch-master-1 (172.31.29.90)
阶段: index
文件迁移进度: 0.6% (17.50 MB/2.90 GB) 速度: 8.74 MB/s
Translog进度: 0.0% (0/71240 ops)
已用时: 4.0s
2024-11-21 12:17:27 - INFO - 源节点: elasticsearch-master-4 -> 目标节点: elasticsearch-master-1 (172.31.29.90)
阶段: translog
文件迁移进度: 100.0% (2.90 GB/2.90 GB) 速度: 0.00 B/s
Translog进度: 84.4% (859665/1018867 ops)
已用时: 701.4s
2024-11-21 12:19:26 - INFO - 源节点: elasticsearch-master-4 -> 目标节点: elasticsearch-master-1 (172.31.29.90)
阶段: translog
文件迁移进度: 100.0% (2.90 GB/2.90 GB) 速度: 0.00 B/s
Translog进度: 100.0% (1018721/1018867 ops)
已用时: 820.4s
2024-11-21 12:20:03 - INFO - 索引 .ds-traces-apm-default-2024.11.21-010484 的分片数量: 3
2024-11-21 12:20:03 - INFO - 等待迁移开始...
2024-11-21 12:20:06 - INFO - 索引 .ds-traces-apm-default-2024.11.21-010484 的分片数量: 3
2024-11-21 12:20:06 - INFO - 等待迁移开始...
2024-11-21 12:20:08 - INFO - 索引 .ds-traces-apm-default-2024.11.21-010484 的分片数量: 3
2024-11-21 12:20:08 - INFO - 分片迁移完成！用时: 861.5 秒
2024-11-21 12:20:08 - INFO - 集群状态正常(green)，迁移成功完成
2024-11-21 12:20:08 - INFO - 分片迁移成功: .ds-traces-apm-default-2024.11.21-010484 分片 2
2024-11-21 12:20:08 - INFO - - 执行总结:
2024-11-21 12:20:08 - INFO - - 扫描的索引: .ds-traces-apm-default-2024.11.21-010484
2024-11-21 12:20:08 - INFO - - 总计迁移分片数: 1
2024-11-21 12:20:08 - INFO - 分片平衡任务完成
2024-11-21 12:20:08 - INFO - 等待下一次检查...
```
