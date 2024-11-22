import requests
import json
from collections import defaultdict
import urllib3
from datetime import datetime
import logging
import time
import os
from tenacity import retry, stop_after_attempt, wait_exponential, before_log, after_log
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志
def setup_logging():
    logger = logging.getLogger('shard_balancer')
    logger.setLevel(logging.INFO)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)

    # 添加处理器
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

# ES配置
# ES配置
ES_HOST = os.getenv('ES_HOST', "https://172.31.87.85:9200")
ES_AUTH = (
    os.getenv('ES_USER', "elastic"),
    os.getenv('ES_PASSWORD', "xxxxxx")
)

class ESError(Exception):
    """自定义ES错误类"""
    pass

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def es_request(method, url, **kwargs):
    """封装的ES请求函数，带重试机制"""
    try:
        response = requests.request(
            method,
            f"{ES_HOST}/{url.lstrip('/')}",
            auth=ES_AUTH,
            verify=False,
            **kwargs
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"ES请求失败: {str(e)}")
        raise ESError(f"ES请求失败: {str(e)}")

def get_latest_apm_index():
    """获取最新的APM索引"""
    try:
        indices = es_request('GET', '/_cat/indices/.ds-traces-apm-*?format=json')
        apm_indices = [idx for idx in indices if idx['index'].startswith('.ds-traces-apm-')]
        if not apm_indices:
            logger.warning("未找到APM索引")
            return None
        latest_index = sorted(apm_indices, key=lambda x: x['index'])[-1]['index']
        logger.info(f"找到最新的APM索引: {latest_index}")
        return latest_index
    except Exception as e:
        logger.error(f"获取最新APM索引失败: {str(e)}")
        raise

def get_node_stats():
    """获取节点状态信息"""
    try:
        nodes = es_request('GET', '/_nodes/stats')['nodes']
        node_stats = {}
        for node_id, stats in nodes.items():
            try:
                node_stats[stats['name']] = {
                    'cpu_percent': stats['os']['cpu']['percent'],
                    'heap_percent': stats['jvm']['mem']['heap_used_percent'],
                    'disk_percent': stats['fs']['total']['available_in_bytes'] / stats['fs']['total']['total_in_bytes'] * 100,
                    'ip': stats['ip']
                }
            except KeyError as e:
                logger.warning(f"节点 {stats.get('name', 'unknown')} 缺少某些统计信息: {str(e)}")

        logger.info(f"成功获取 {len(node_stats)} 个节点的状态信息")
        return node_stats
    except Exception as e:
        logger.error(f"获取节点状态失败: {str(e)}")
        raise

def get_shards_allocation(index_name):
    """获取分片分配情况"""
    try:
        shards = es_request('GET', f'/_cat/shards/{index_name}?format=json')
        logger.info(f"索引 {index_name} 的分片数量: {len(shards)}")
        return shards
    except Exception as e:
        logger.error(f"获取分片分配情况失败: {str(e)}")
        raise

def find_best_target_node(node_stats, current_node, node_shards, already_assigned=None):
    """找到综合评分最优的节点（考虑负载和主分片数量）
    
    Args:
        node_stats: 节点状态信息
        current_node: 当前节点名称
        node_shards: 节点分片分布情况
        already_assigned: 已经分配给各节点的分片数量字典
    """
    try:
        min_score = float('inf')
        target_node = None
        
        # 初始化已分配分片计数
        if already_assigned is None:
            already_assigned = defaultdict(int)

        # 获取每个节点的主分片数量
        shard_counts = {node: len(shards) for node, shards in node_shards.items()}
        # 加上已经分配的分片数量
        for node, count in already_assigned.items():
            shard_counts[node] = shard_counts.get(node, 0) + count
            
        max_shards = max(shard_counts.values()) if shard_counts else 1

        for node_name, stats in node_stats.items():
            if node_name == current_node:
                continue

            # 计算负载分数 (0-100)
            load_score = (
                stats['cpu_percent'] * 0.4 +
                stats['heap_percent'] * 0.4 +
                (100 - stats['disk_percent']) * 0.2
            )

            # 计算分片数量分数 (0-100)
            shard_count = shard_counts.get(node_name, 0)
            shard_score = (shard_count / max_shards) * 100

            # 综合评分 (负载权重0.6，分片数量权重0.4)
            final_score = load_score * 0.6 + shard_score * 0.4

            logger.debug(
                f"节点 {node_name} 评分详情:\n"
                f"  - 负载分数: {load_score:.2f}\n"
                f"  - 主分片数: {shard_count} (分数: {shard_score:.2f})\n"
                f"  - 综合评分: {final_score:.2f}"
            )

            if final_score < min_score:
                min_score = final_score
                target_node = node_name

        if target_node:
            logger.info(
                f"找到最佳目标节点: {target_node}\n"
                f"  - CPU使用率: {node_stats[target_node]['cpu_percent']}%\n"
                f"  - 堆内存使用率: {node_stats[target_node]['heap_percent']}%\n"
                f"  - 磁盘使用率: {100 - node_stats[target_node]['disk_percent']:.1f}%\n"
                f"  - 当前主分片数: {shard_counts.get(target_node, 0)}\n"
                f"  - 综合评分: {min_score:.2f}"
            )
        else:
            logger.warning("未找到合适的目标节点")

        return target_node
    except Exception as e:
        logger.error(f"查找目标节点失败: {str(e)}")
        raise

def get_recovery_status(index_name):
    """获取索引恢复状态"""
    try:
        return es_request('GET', f'/_cat/recovery/{index_name}?format=json&active_only=true')
    except Exception as e:
        logger.error(f"获取恢复状态失败: {str(e)}")
        raise

def format_bytes(bytes_num):
    """格式化字节数为人类可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_num < 1024:
            return f"{bytes_num:.2f} {unit}"
        bytes_num /= 1024
    return f"{bytes_num:.2f} TB"

def parse_size(size_str):
    """解析ES返回的大小字符串为字节数"""
    if not size_str or size_str == '0b':
        return 0
    
    # 统一转换为小写
    size_str = size_str.lower()
    
    # 定义单位转换表（转换到字节）
    units = {
        'b': 1,
        'kb': 1024,
        'mb': 1024 * 1024,
        'gb': 1024 * 1024 * 1024,
        'tb': 1024 * 1024 * 1024 * 1024
    }
    
    try:
        # 分离数字和单位
        number = float(''.join(c for c in size_str if c.isdigit() or c == '.'))
        unit = ''.join(c for c in size_str if c.isalpha())
        
        # 转换为字节
        if unit in units:
            return number * units[unit]
        else:
            logger.warning(f"未知的大小单位: {unit}")
            return 0
    except ValueError as e:
        logger.warning(f"解析大小字符串失败: {size_str} - {str(e)}")
        return 0
    
def monitor_migration(index_name, shard_num, target_node):
    """监控特定分片的迁移进度"""
    start_time = time.time()
    last_progress = -1
    last_bytes_recovered = 0
    last_translog_progress = -1

    logger.info(f"开始监控分片迁移进度 - 索引: {index_name}, 分片: {shard_num}, 目标节点: {target_node}")

    while True:
        try:
            recoveries = get_recovery_status(index_name)

            # 过滤出当前正在迁移的分片
            current_recovery = None
            for recovery in recoveries:
                if (int(recovery.get('shard', -1)) == shard_num and 
                    (recovery.get('target_node') == target_node or 
                     recovery.get('target_host') == node_stats.get(target_node, {}).get('ip'))):
                    current_recovery = recovery
                    break

            if not current_recovery:
                # 检查分片是否已经在目标节点上
                shards = get_shards_allocation(index_name)
                target_shard = None
                for shard in shards:
                    if (int(shard['shard']) == shard_num and 
                        shard['prirep'] == 'p' and 
                        shard['node'] == target_node):
                        target_shard = shard
                        break

                if target_shard:
                    elapsed_time = time.time() - start_time
                    logger.info(f"分片迁移完成！用时: {elapsed_time:.1f} 秒")
                    return True
                else:
                    logger.info("等待迁移开始...")
                    time.sleep(2)
                    continue

            # 获取文件恢复进度
            bytes_recovered = parse_size(current_recovery.get('bytes_recovered', '0b'))
            bytes_total = parse_size(current_recovery.get('bytes_total', '0b'))
            
            # 获取 translog 恢复进度
            translog_ops_recovered = int(current_recovery.get('translog_ops_recovered', 0))
            translog_ops_total = int(current_recovery.get('translog_ops', 0))
            stage = current_recovery.get('stage', '')

            # 计算文件恢复进度
            if bytes_total > 0:
                current_progress = (bytes_recovered / bytes_total) * 100
            else:
                current_progress = 100.0

            # 计算 translog 恢复进度
            if translog_ops_total > 0:
                translog_progress = (translog_ops_recovered / translog_ops_total) * 100
            else:
                translog_progress = 100.0

            elapsed_time = time.time() - start_time

            # 计算速度
            bytes_delta = bytes_recovered - last_bytes_recovered
            if elapsed_time > 0:
                current_speed = bytes_delta / 2
            else:
                current_speed = 0

            # 只有当进度发生变化时才输出日志
            if (current_progress != last_progress or 
                translog_progress != last_translog_progress):
                
                status_message = (
                    f"源节点: {current_recovery.get('source_node')} -> "
                    f"目标节点: {current_recovery.get('target_node')} ({current_recovery.get('target_host')})\n"
                    f"阶段: {stage}\n"
                    f"文件迁移进度: {current_progress:.1f}% "
                    f"({format_bytes(bytes_recovered)}/{format_bytes(bytes_total)}) "
                    f"速度: {format_bytes(current_speed)}/s\n"
                    f"Translog进度: {translog_progress:.1f}% "
                    f"({translog_ops_recovered}/{translog_ops_total} ops)\n"
                    f"已用时: {elapsed_time:.1f}s"
                )
                logger.info(status_message)
                
                last_progress = current_progress
                last_translog_progress = translog_progress
                last_bytes_recovered = bytes_recovered

            if (current_progress >= 100 and translog_progress >= 100 and 
                stage == "done"):
                logger.info("迁移完全完成！")
                return True

            time.sleep(10)

        except Exception as e:
            logger.error(f"监控过程中发生错误: {str(e)}")
            time.sleep(2)

def move_shard(index_name, shard_num, from_node, to_node):
    """移动分片到目标节点"""
    try:
        payload = {
            "commands": [{
                "move": {
                    "index": index_name,
                    "shard": shard_num,
                    "from_node": from_node,
                    "to_node": to_node
                }
            }]
        }

        logger.info(f"开始移动分片: 从 {from_node} 到 {to_node}")
        result = es_request(
            'POST',
            '/_cluster/reroute',
            json=payload,
            headers={"Content-Type": "application/json"}
        )

        # 启动监控
        success = monitor_migration(index_name, shard_num, to_node)
        
        if success:
            # 检查集群健康状态
            final_health = es_request('GET', '/_cluster/health')
            if final_health['status'] == 'green':
                logger.info("集群状态正常(green)，迁移成功完成")
            else:
                logger.warning(f"迁移完成，但集群状态为: {final_health['status']}")
        
        return result

    except Exception as e:
        logger.error(f"移动分片失败: {str(e)}")
        raise

def get_node_load_details(node_stats, node_name):
    """获取节点的详细负载信息"""
    stats = node_stats[node_name]
    return {
        'cpu': f"{stats['cpu_percent']}%",
        'heap': f"{stats['heap_percent']}%",
        'disk': f"{100 - stats['disk_percent']:.1f}%"
    }

def print_migration_details(shard_info, from_node, to_node, node_stats):
    """打印迁移详情"""
    from_load = get_node_load_details(node_stats, from_node)
    to_load = get_node_load_details(node_stats, to_node)
    
    logger.info("\n" + "="*80)
    logger.info("分片迁移详情:")
    logger.info(f"索引名称: {shard_info['index']}")
    logger.info(f"分片编号: {shard_info['shard']}")
    logger.info(f"从节点: {from_node}")
    logger.info(f"  - CPU使用率: {from_load['cpu']}")
    logger.info(f"  - 堆内存使用率: {from_load['heap']}")
    logger.info(f"  - 磁盘使用率: {from_load['disk']}")
    logger.info(f"迁移到节点: {to_node}")
    logger.info(f"  - CPU使用率: {to_load['cpu']}")
    logger.info(f"  - 堆内存使用率: {to_load['heap']}")
    logger.info(f"  - 磁盘使用率: {to_load['disk']}")
    logger.info("迁移原因: 源节点存在多个主分片，需要进行负载均衡")
    logger.info("=" * 80 + "\n")

def main():
    try:
        logger.info("启动分片平衡服务")
        
        while True:
            try:
                logger.info("\n" + "="*80)
                logger.info("开始新一轮分片平衡检查...")
                
                # 获取最新的APM索引
                latest_index = get_latest_apm_index()
                if not latest_index:
                    logger.info("未找到需要处理的索引，等待下一次检查...")
                    time.sleep(60)
                    continue
                
                # 获取分片分配情况
                shards = get_shards_allocation(latest_index)
                
                # 按节点分组的主分片
                node_shards = defaultdict(list)
                for shard in shards:
                    if shard['prirep'] == 'p':  # 只处理主分片
                        node_shards[shard['node']].append({
                            'shard': int(shard['shard']),
                            'index': shard['index'],
                            'size': shard.get('store', 'N/A'),
                            'docs': shard.get('docs', 'N/A')
                        })
                
                # 获取节点状态
                node_stats = get_node_stats()
                
                # 打印当前集群状态摘要
                logger.info("\n当前集群状态摘要:")
                for node_name, stats in node_stats.items():
                    load = get_node_load_details(node_stats, node_name)
                    logger.info(f"节点 {node_name}:")
                    logger.info(f"  - 主分片数量: {len(node_shards[node_name])}")
                    logger.info(f"  - CPU使用率: {load['cpu']}")
                    logger.info(f"  - 堆内存使用率: {load['heap']}")
                    logger.info(f"  - 磁盘使用率: {load['disk']}")
                    logger.info("-" * 40)
                
                # 检查和处理不平衡的分片
                migration_count = 0
                for node, shards_list in node_shards.items():
                    if len(shards_list) > 1:
                        logger.info(f"\n发现节点 {node} 有 {len(shards_list)} 个主分片，开始进行负载均衡...")
                        
                        # 跟踪已分配的分片
                        already_assigned = defaultdict(int)
                        
                        # 移动除第一个之外的所有分片
                        for shard in shards_list[1:]:
                            try:
                                # 考虑已分配的分片找到最佳目标节点
                                target_node = find_best_target_node(
                                    node_stats, 
                                    node, 
                                    node_shards,
                                    already_assigned
                                )
                                
                                if not target_node:
                                    continue
                                    
                                # 记录这次分配
                                already_assigned[target_node] += 1
                                
                                # 打印迁移详情
                                print_migration_details(shard, node, target_node, node_stats)
                                
                                result = move_shard(
                                    shard['index'],
                                    shard['shard'],
                                    node,
                                    target_node
                                )
                                
                                migration_count += 1
                                logger.info(f"分片迁移成功: {shard['index']} 分片 {shard['shard']}")
                                
                            except Exception as e:
                                logger.error(f"移动分片时发生错误: {str(e)}")
                                continue
                
                # 打印总结
                logger.info(f"\n本轮执行总结:")
                logger.info(f"- 扫描的索引: {latest_index}")
                logger.info(f"- 总计迁移分片数: {migration_count}")
                if migration_count == 0:
                    logger.info("- 未发现需要平衡的分片")
                
                logger.info("\n等待下一次检查...")
                logger.info("=" * 80)
                
                # 等待1分钟
                time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("\n收到终止信号，正在退出...")
                break
            except Exception as e:
                logger.error(f"本轮检查过程中发生错误: {str(e)}")
                logger.info("等待下一次检查...")
                time.sleep(60)
                continue
                
    except KeyboardInterrupt:
        logger.info("\n收到终止信号，正在退出...")
    except Exception as e:
        logger.error(f"服务运行过程中发生错误: {str(e)}")
    finally:
        logger.info("分片平衡服务已停止")

if __name__ == "__main__":
    main()
