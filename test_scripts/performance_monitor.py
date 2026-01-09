#!/usr/bin/env python3
"""
性能监控脚本 - 实时监控分布式存储系统性能
监控 Master 和 Worker 节点的运行状态
"""

import requests
import time
import json
import argparse
from datetime import datetime

class PerformanceMonitor:
    def __init__(self, master_url="http://localhost:8080", interval=5):
        self.master_url = master_url
        self.interval = interval
        self.metrics_history = []

    def get_master_stats(self):
        """获取 Master 统计信息"""
        try:
            response = requests.get(f"{self.master_url}/stats", timeout=2)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"获取 Master 统计信息失败: {e}")
        return None

    def get_master_metrics(self):
        """获取 Prometheus 格式的指标"""
        try:
            response = requests.get(f"{self.master_url}/metrics", timeout=2)
            if response.status_code == 200:
                return self.parse_prometheus_metrics(response.text)
        except Exception as e:
            print(f"获取 Master 指标失败: {e}")
        return None

    def parse_prometheus_metrics(self, metrics_text):
        """解析 Prometheus 格式的指标"""
        metrics = {}
        for line in metrics_text.split('\n'):
            if line and not line.startswith('#'):
                parts = line.split()
                if len(parts) == 2:
                    name = parts[0]
                    value = parts[1]
                    try:
                        metrics[name] = float(value)
                    except ValueError:
                        pass
        return metrics

    def get_worker_stats(self, worker_url):
        """获取 Worker 统计信息"""
        try:
            response = requests.get(f"{worker_url}/metrics", timeout=2)
            if response.status_code == 200:
                return self.parse_prometheus_metrics(response.text)
        except Exception as e:
            print(f"获取 Worker {worker_url} 统计信息失败: {e}")
        return None

    def monitor(self, duration=0):
        """开始监控"""
        print("=" * 80)
        print("分布式存储系统性能监控")
        print("=" * 80)
        print(f"Master URL: {self.master_url}")
        print(f"监控间隔: {self.interval}秒")
        if duration > 0:
            print(f"监控时长: {duration}秒")
        print("")

        start_time = time.time()
        iteration = 0

        try:
            while True:
                iteration += 1
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                print(f"\n[{iteration}] {current_time}")
                print("-" * 80)

                # 获取 Master 统计
                stats = self.get_master_stats()
                if stats:
                    print(f"Master 统计:")
                    print(f"  活跃节点: {stats.get('active_nodes', 0)}")
                    print(f"  文件总数: {stats.get('total_files', 0)}")
                    print(f"  副本不足: {stats.get('under_replicated_files', 0)}")
                    print(f"  哈希环大小: {stats.get('ring_size', 0)}")
                else:
                    print("Master 统计: 获取失败")

                # 获取 Master Prometheus 指标
                metrics = self.get_master_metrics()
                if metrics:
                    print(f"\nMaster 指标:")
                    print(f"  系统状态: {metrics.get('mdfs_up', 0)}")
                    print(f"  活跃节点数: {metrics.get('mdfs_active_nodes', 0)}")
                    print(f"  文件总数: {metrics.get('mdfs_total_files', 0)}")
                    print(f"  副本不足文件: {metrics.get('mdfs_under_replicated_files', 0)}")

                # 获取 Worker 统计 (假设有3个worker)
                worker_urls = [
                    "http://localhost:8081",
                    "http://localhost:8082",
                    "http://localhost:8083"
                ]

                print(f"\nWorker 状态:")
                for i, worker_url in enumerate(worker_urls, 1):
                    worker_metrics = self.get_worker_stats(worker_url)
                    if worker_metrics:
                        print(f"  Worker {i} ({worker_url}):")
                        print(f"    文件数: {worker_metrics.get('mdfs_worker_files', 0)}")
                        print(f"    存储量: {worker_metrics.get('mdfs_worker_bytes_total', 0) / 1024 / 1024:.2f} MB")
                        print(f"    运行状态: {worker_metrics.get('mdfs_worker_up', 0)}")
                    else:
                        print(f"  Worker {i} ({worker_url}): 离线")

                # 保存指标历史
                metrics_data = {
                    'timestamp': datetime.now().isoformat(),
                    'iteration': iteration,
                    'master_stats': stats,
                    'master_metrics': metrics,
                    'worker_metrics': {}
                }

                for i, worker_url in enumerate(worker_urls, 1):
                    worker_metrics = self.get_worker_stats(worker_url)
                    if worker_metrics:
                        metrics_data['worker_metrics'][f'worker_{i}'] = worker_metrics

                self.metrics_history.append(metrics_data)

                # 检查是否达到监控时长
                if duration > 0 and (time.time() - start_time) >= duration:
                    print(f"\n已达到监控时长 {duration} 秒，停止监控")
                    break

                # 等待下一次监控
                time.sleep(self.interval)

        except KeyboardInterrupt:
            print("\n\n用户中断监控")

        print("\n" + "=" * 80)
        print("监控结束")
        print("=" * 80)

    def save_metrics(self, output_file):
        """保存监控数据到文件"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.metrics_history, f, indent=2, ensure_ascii=False)
        print(f"监控数据已保存到: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='性能监控脚本 - 监控分布式存储系统')
    parser.add_argument('--master_url', default='http://localhost:8080',
                       help='Master 服务 URL (默认: http://localhost:8080)')
    parser.add_argument('--interval', type=int, default=5,
                       help='监控间隔（秒）(默认: 5)')
    parser.add_argument('--duration', type=int, default=0,
                       help='监控时长（秒），0=无限期 (默认: 0)')
    parser.add_argument('--output', default='../test_videos/logs/monitor_data.json',
                       help='监控数据输出文件 (默认: ../test_videos/logs/monitor_data.json)')

    args = parser.parse_args()

    monitor = PerformanceMonitor(args.master_url, args.interval)
    monitor.monitor(args.duration)

    if monitor.metrics_history:
        monitor.save_metrics(args.output)

if __name__ == '__main__':
    main()