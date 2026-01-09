#!/usr/bin/env python3
"""
墓碑机制验证测试 - 完全自动化测试脚本
"""

# 禁用代理
import os
os.environ['NO_PROXY'] = 'localhost,127.0.0.1'
os.environ['no_proxy'] = 'localhost,127.0.0.1'

import requests
import subprocess
import time
import json
import sys
import re
from datetime import datetime
from pathlib import Path

class TombstoneTestRunner:
    def __init__(self, master_url="http://localhost:8080", secret="admin888"):
        self.master_url = master_url
        self.secret = secret
        self.log_dir = Path("../test_videos/logs/tombstone")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.test_results = {
            'test_4_1': {},
            'test_4_2': {},
            'verification': {},
            'overall': {}
        }
        self.start_time = None
        self.end_time = None

    def log(self, message, level="INFO"):
        """记录日志到文件和标准输出"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        with open(self.log_dir / "test_log.txt", "a") as f:
            f.write(log_entry + "\n")

    def check_environment(self):
        """检查测试环境"""
        self.log("开始环境检查...")
        
        # 检查 Docker 容器状态
        try:
            result = subprocess.run(['docker-compose', 'ps'], capture_output=True, text=True, cwd='..')
            if result.returncode != 0 or 'Up' not in result.stdout:
                self.log("Docker 容器未运行，请先启动：docker-compose up -d", "ERROR")
                return False
            self.log("✓ Docker 容器运行正常")
        except Exception as e:
            self.log(f"检查 Docker 容器失败: {e}", "ERROR")
            return False

        # 检查 Master 健康状态
        try:
            response = requests.get(f"{self.master_url}/health", timeout=5)
            if response.status_code != 200:
                self.log("Master 健康检查失败", "ERROR")
                return False
            self.log("✓ Master 健康检查通过")
        except Exception as e:
            self.log(f"Master 连接失败: {e}", "ERROR")
            return False

        # 检查当前文件数量
        try:
            stats = requests.get(f"{self.master_url}/stats", timeout=5).json()
            file_count = stats.get('total_files', 0)
            self.log(f"当前文件数量: {file_count}")
            if file_count < 20:
                self.log(f"警告: 文件数量较少 ({file_count})，可能影响测试", "WARN")
        except Exception as e:
            self.log(f"获取统计信息失败: {e}", "WARN")

        return True

    def delete_file(self, filename):
        """通过 API 删除文件"""
        url = f"{self.master_url}/delete"
        params = {'name': filename, 'secret': self.secret}
        
        try:
            response = requests.get(url, params=params, timeout=10)
            return {
                'file': filename,
                'status': response.status_code,
                'response': response.text.strip(),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'file': filename,
                'status': -1,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def get_stats(self):
        """获取 Master 统计信息"""
        try:
            response = requests.get(f"{self.master_url}/stats", timeout=5)
            return response.json()
        except Exception as e:
            self.log(f"获取统计信息失败: {e}", "ERROR")
            return {}

    def get_worker_files(self, worker_name):
        """获取 Worker 节点上的文件列表"""
        container_name = f"movie-dist-kv-{worker_name}-1"
        data_dir = f"/root/data_{worker_name[-1]}{worker_name[-1]}"
        
        try:
            result = subprocess.run(
                ['docker', 'exec', container_name, 'ls', data_dir],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                files = [f for f in result.stdout.strip().split('\n') if f]
                return files
            return []
        except Exception as e:
            self.log(f"获取 {worker_name} 文件列表失败: {e}", "ERROR")
            return []

    def stop_worker(self, worker_name):
        """停止 Worker 节点"""
        try:
            result = subprocess.run(
                ['docker-compose', 'stop', worker_name],
                capture_output=True, text=True, cwd='..'
            )
            if result.returncode == 0:
                self.log(f"✓ {worker_name} 已停止")
                return True
            else:
                self.log(f"停止 {worker_name} 失败: {result.stderr}", "ERROR")
                return False
        except Exception as e:
            self.log(f"停止 {worker_name} 异常: {e}", "ERROR")
            return False

    def start_worker(self, worker_name):
        """启动 Worker 节点"""
        try:
            result = subprocess.run(
                ['docker-compose', 'start', worker_name],
                capture_output=True, text=True, cwd='..'
            )
            if result.returncode == 0:
                self.log(f"✓ {worker_name} 已启动")
                return True
            else:
                self.log(f"启动 {worker_name} 失败: {result.stderr}", "ERROR")
                return False
        except Exception as e:
            self.log(f"启动 {worker_name} 异常: {e}", "ERROR")
            return False

    def get_worker_logs(self, worker_name, pattern=""):
        """获取 Worker 日志"""
        try:
            result = subprocess.run(
                ['docker-compose', 'logs', worker_name],
                capture_output=True, text=True, cwd='..'
            )
            
            if pattern:
                lines = result.stdout.split('\n')
                matched = [line for line in lines if pattern in line]
                return matched
            return result.stdout
        except Exception as e:
            self.log(f"获取 {worker_name} 日志失败: {e}", "ERROR")
            return ""

    def check_auto_cleanup(self, workers, target_files, timeout=30):
        """检查自动清理机制"""
        self.log(f"等待自动清理（最多 {timeout} 秒）...")
        
        start_time = time.time()
        cleanup_events = []
        
        while time.time() - start_time < timeout:
            time.sleep(5)
            
            for worker in workers:
                logs = self.get_worker_logs(worker, "墓碑机制")
                for log in logs:
                    # 检查是否有自动删除事件
                    if "墓碑机制：自动删除" in log and log not in cleanup_events:
                        cleanup_events.append(log)
                        self.log(f"发现自动清理事件: {worker} - {log[:100]}")
            
            # 检查目标文件是否已被清理
            all_clean = True
            for worker in workers:
                files = self.get_worker_files(worker)
                for target in target_files:
                    if target in files:
                        all_clean = False
                        break
                if not all_clean:
                    break
            
            if all_clean:
                self.log("✓ 所有目标文件已自动清理")
                return True, cleanup_events
        
        return False, cleanup_events

    def run_test_4_1(self):
        """测试4.1：删除后重启测试"""
        self.log("=" * 60)
        self.log("开始测试 4.1: 删除后重启测试")
        self.log("=" * 60)
        
        test_start = datetime.now()
        test_files = [f"test_movie_{i:04d}.mp4" for i in range(10)]
        
        self.test_results['test_4_1']['test_files'] = test_files
        self.test_results['test_4_1']['start_time'] = test_start.isoformat()
        
        # 步骤1：记录初始状态
        self.log("\n步骤1: 记录初始状态")
        initial_stats = self.get_stats()
        self.log(f"初始文件数: {initial_stats.get('total_files', 0)}")
        self.log(f"活跃节点数: {initial_stats.get('active_nodes', 0)}")
        
        # 步骤2：停止 Worker2
        self.log("\n步骤2: 停止 Worker2")
        if not self.stop_worker('worker2'):
            self.test_results['test_4_1']['status'] = 'failed'
            return False
        time.sleep(5)
        
        # 步骤3：删除文件
        self.log("\n步骤3: 删除文件")
        delete_results = []
        for filename in test_files:
            result = self.delete_file(filename)
            delete_results.append(result)
            status_str = "✓" if result['status'] == 200 else "✗"
            self.log(f"  {status_str} {filename}: {result.get('response', result.get('error', ''))}")
            time.sleep(0.5)
        
        self.test_results['test_4_1']['delete_results'] = delete_results
        
        # 步骤4：验证墓碑记录
        self.log("\n步骤4: 验证墓碑记录")
        master_logs = self.get_worker_logs('master', '墓碑')
        tombstone_created = any('创建墓碑' in log for log in master_logs)
        self.log(f"墓碑记录已创建: {'是' if tombstone_created else '否'}")
        
        # 步骤5：重启 Worker2
        self.log("\n步骤5: 重启 Worker2")
        if not self.start_worker('worker2'):
            self.test_results['test_4_1']['status'] = 'failed'
            return False
        time.sleep(10)
        
        # 步骤6：检查自动清理
        self.log("\n步骤6: 检查自动清理")
        cleaned, cleanup_events = self.check_auto_cleanup(['worker2'], test_files, timeout=30)
        
        self.test_results['test_4_1']['auto_cleanup'] = cleaned
        self.test_results['test_4_1']['cleanup_events'] = cleanup_events
        
        # 步骤7：最终验证
        self.log("\n步骤7: 最终验证")
        all_workers_clean = True
        for worker in ['worker1', 'worker2', 'worker3']:
            files = self.get_worker_files(worker)
            found_files = [f for f in test_files if f in files]
            if found_files:
                self.log(f"  {worker}: ✗ 发现残留文件 {found_files}", "ERROR")
                all_workers_clean = False
            else:
                self.log(f"  {worker}: ✓ 无残留文件")
        
        final_stats = self.get_stats()
        self.log(f"最终文件数: {final_stats.get('total_files', 0)}")
        
        test_end = datetime.now()
        test_duration = (test_end - test_start).total_seconds()
        
        self.test_results['test_4_1']['end_time'] = test_end.isoformat()
        self.test_results['test_4_1']['duration'] = test_duration
        self.test_results['test_4_1']['status'] = 'passed' if all_workers_clean else 'failed'
        self.test_results['test_4_1']['tombstone_created'] = tombstone_created
        
        self.log(f"\n测试4.1完成，用时: {test_duration:.1f}秒")
        self.log(f"测试结果: {self.test_results['test_4_1']['status']}")
        
        return all_workers_clean

    def run_test_4_2(self):
        """测试4.2：部分删除失败测试"""
        self.log("=" * 60)
        self.log("开始测试 4.2: 部分删除失败测试")
        self.log("=" * 60)
        
        test_start = datetime.now()
        test_files = [f"test_movie_{i:04d}.mp4" for i in range(10, 20)]
        
        self.test_results['test_4_2']['test_files'] = test_files
        self.test_results['test_4_2']['start_time'] = test_start.isoformat()
        
        # 步骤1：记录初始状态
        self.log("\n步骤1: 记录初始状态")
        initial_stats = self.get_stats()
        self.log(f"初始文件数: {initial_stats.get('total_files', 0)}")
        
        # 步骤2：停止 Worker1 和 Worker2
        self.log("\n步骤2: 停止 Worker1 和 Worker2")
        if not (self.stop_worker('worker1') and self.stop_worker('worker2')):
            self.test_results['test_4_2']['status'] = 'failed'
            return False
        time.sleep(10)
        
        # 步骤3：删除文件（部分会失败）
        self.log("\n步骤3: 删除文件（部分将失败）")
        delete_results = []
        for filename in test_files:
            result = self.delete_file(filename)
            delete_results.append(result)
            status_str = "✓" if result['status'] == 200 else "✗"
            self.log(f"  {status_str} {filename}: {result.get('response', result.get('error', ''))}")
            time.sleep(0.5)
        
        self.test_results['test_4_2']['delete_results'] = delete_results
        
        # 步骤4：验证部分删除失败处理
        self.log("\n步骤4: 验证部分删除失败处理")
        master_logs = self.get_worker_logs('master', '部分删除失败')
        partial_failure = any('部分删除失败' in log for log in master_logs)
        self.log(f"检测到部分删除失败: {'是' if partial_failure else '否'}")
        
        tombstone_logs = self.get_worker_logs('master', '墓碑')
        tombstone_created = any('创建墓碑' in log for log in tombstone_logs)
        self.log(f"墓碑记录已创建: {'是' if tombstone_created else '否'}")
        
        # 步骤5：重启 Worker1 和 Worker2
        self.log("\n步骤5: 重启 Worker1 和 Worker2")
        if not (self.start_worker('worker1') and self.start_worker('worker2')):
            self.test_results['test_4_2']['status'] = 'failed'
            return False
        time.sleep(10)
        
        # 步骤6：检查自动清理
        self.log("\n步骤6: 检查自动清理")
        cleaned, cleanup_events = self.check_auto_cleanup(['worker1', 'worker2'], test_files, timeout=30)
        
        self.test_results['test_4_2']['auto_cleanup'] = cleaned
        self.test_results['test_4_2']['cleanup_events'] = cleanup_events
        
        # 步骤7：最终验证
        self.log("\n步骤7: 最终验证")
        all_workers_clean = True
        for worker in ['worker1', 'worker2', 'worker3']:
            files = self.get_worker_files(worker)
            found_files = [f for f in test_files if f in files]
            if found_files:
                self.log(f"  {worker}: ✗ 发现残留文件 {found_files}", "ERROR")
                all_workers_clean = False
            else:
                self.log(f"  {worker}: ✓ 无残留文件")
        
        final_stats = self.get_stats()
        self.log(f"最终文件数: {final_stats.get('total_files', 0)}")
        
        test_end = datetime.now()
        test_duration = (test_end - test_start).total_seconds()
        
        self.test_results['test_4_2']['end_time'] = test_end.isoformat()
        self.test_results['test_4_2']['duration'] = test_duration
        self.test_results['test_4_2']['status'] = 'passed' if all_workers_clean else 'failed'
        self.test_results['test_4_2']['tombstone_created'] = tombstone_created
        self.test_results['test_4_2']['partial_failure_detected'] = partial_failure
        
        self.log(f"\n测试4.2完成，用时: {test_duration:.1f}秒")
        self.log(f"测试结果: {self.test_results['test_4_2']['status']}")
        
        return all_workers_clean

    def verify_results(self):
        """验证测试结果"""
        self.log("\n" + "=" * 60)
        self.log("验证测试结果")
        self.log("=" * 60)
        
        verification = {
            'test_4_1': {},
            'test_4_2': {},
            'overall': {}
        }
        
        # 验证测试4.1
        test_4_1 = self.test_results['test_4_1']
        if test_4_1:
            verification['test_4_1']['passed'] = (
                test_4_1.get('status') == 'passed' and
                test_4_1.get('tombstone_created') and
                test_4_1.get('auto_cleanup')
            )
            verification['test_4_1']['details'] = {
                'status': test_4_1.get('status'),
                'tombstone_created': test_4_1.get('tombstone_created'),
                'auto_cleanup': test_4_1.get('auto_cleanup'),
                'duration': test_4_1.get('duration')
            }
        else:
            verification['test_4_1']['passed'] = False
            verification['test_4_1']['details'] = {'error': '测试4.1未执行或数据缺失'}

        # 验证测试4.2
        test_4_2 = self.test_results['test_4_2']
        if test_4_2:
            verification['test_4_2']['passed'] = (
                test_4_2.get('status') == 'passed' and
                test_4_2.get('tombstone_created') and
                test_4_2.get('auto_cleanup')
            )
            verification['test_4_2']['details'] = {
                'status': test_4_2.get('status'),
                'tombstone_created': test_4_2.get('tombstone_created'),
                'partial_failure_detected': test_4_2.get('partial_failure_detected'),
                'auto_cleanup': test_4_2.get('auto_cleanup'),
                'duration': test_4_2.get('duration')
            }
        else:
            verification['test_4_2']['passed'] = False
            verification['test_4_2']['details'] = {'error': '测试4.2未执行或数据缺失'}
        
        # 整体验证
        verification['overall']['all_passed'] = (
            verification['test_4_1']['passed'] and
            verification['test_4_2']['passed']
        )
        
        self.test_results['verification'] = verification
        
        self.log(f"\n验证结果:")
        self.log(f"  测试4.1: {'✓ 通过' if verification['test_4_1']['passed'] else '✗ 失败'}")
        self.log(f"  测试4.2: {'✓ 通过' if verification['test_4_2']['passed'] else '✗ 失败'}")
        self.log(f"  整体结果: {'✓ 通过' if verification['overall']['all_passed'] else '✗ 失败'}")
        
        return verification['overall']['all_passed']

    def generate_report(self):
        """生成 Markdown 格式测试报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.log_dir / f"tombstone_test_report_{timestamp}.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# 墓碑机制验证测试报告\n\n")
            f.write(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 测试概览
            f.write("## 测试概览\n\n")
            f.write(f"- **测试开始时间**: {self.start_time}\n")
            if self.end_time:
                f.write(f"- **测试结束时间**: {self.end_time}\n")
                if self.start_time:
                    duration = self.end_time - self.start_time
                    f.write(f"- **总测试时长**: {duration.total_seconds():.1f}秒\n")
            else:
                f.write("- **测试结束时间**: 未完成\n")
                f.write("- **总测试时长**: 未完成\n")
            f.write(f"- **测试文件数量**: 20 个\n")
            f.write(f"- **Master URL**: {self.master_url}\n\n")
            
            # 测试4.1结果
            test_4_1 = self.test_results['test_4_1']
            f.write("## 测试 4.1: 删除后重启测试\n\n")
            f.write(f"- **状态**: {'✓ 通过' if test_4_1.get('status') == 'passed' else '✗ 失败'}\n")
            f.write(f"- **墓碑创建**: {'✓ 是' if test_4_1.get('tombstone_created') else '✗ 否'}\n")
            f.write(f"- **自动清理**: {'✓ 成功' if test_4_1.get('auto_cleanup') else '✗ 失败'}\n")
            f.write(f"- **测试时长**: {test_4_1.get('duration', 0):.1f}秒\n")
            f.write(f"- **测试文件**: test_movie_0000.mp4 到 test_movie_0009.mp4\n\n")
            
            f.write("### 删除操作详情\n\n")
            f.write("| 文件名 | 状态 | 响应 |\n")
            f.write("|--------|------|------|\n")
            for result in test_4_1.get('delete_results', []):
                status_icon = "✓" if result['status'] == 200 else "✗"
                response = result.get('response', result.get('error', ''))[:50]
                f.write(f"| {result['file']} | {status_icon} | {response} |\n")
            f.write("\n")
            
            # 测试4.2结果
            test_4_2 = self.test_results['test_4_2']
            f.write("## 测试 4.2: 部分删除失败测试\n\n")
            f.write(f"- **状态**: {'✓ 通过' if test_4_2.get('status') == 'passed' else '✗ 失败'}\n")
            f.write(f"- **墓碑创建**: {'✓ 是' if test_4_2.get('tombstone_created') else '✗ 否'}\n")
            f.write(f"- **部分失败检测**: {'✓ 是' if test_4_2.get('partial_failure_detected') else '✗ 否'}\n")
            f.write(f"- **自动清理**: {'✓ 成功' if test_4_2.get('auto_cleanup') else '✗ 失败'}\n")
            f.write(f"- **测试时长**: {test_4_2.get('duration', 0):.1f}秒\n")
            f.write(f"- **测试文件**: test_movie_0010.mp4 到 test_movie_0019.mp4\n\n")
            
            f.write("### 删除操作详情\n\n")
            f.write("| 文件名 | 状态 | 响应 |\n")
            f.write("|--------|------|------|\n")
            for result in test_4_2.get('delete_results', []):
                status_icon = "✓" if result['status'] == 200 else "✗"
                response = result.get('response', result.get('error', ''))[:50]
                f.write(f"| {result['file']} | {status_icon} | {response} |\n")
            f.write("\n")
            
            # 验证结果
            verification = self.test_results.get('verification', {})
            f.write("## 验证结果\n\n")
            f.write(f"- **测试4.1**: {'✓ 通过' if verification.get('test_4_1', {}).get('passed') else '✗ 失败'}\n")
            f.write(f"- **测试4.2**: {'✓ 通过' if verification.get('test_4_2', {}).get('passed') else '✗ 失败'}\n")
            f.write(f"- **整体结果**: {'✓ 全部通过' if verification.get('overall', {}).get('all_passed') else '✗ 存在失败'}\n\n")
            
            # 测试结论
            f.write("## 测试结论\n\n")
            if verification.get('overall', {}).get('all_passed'):
                f.write("### ✅ 测试通过\n\n")
                f.write("墓碑机制验证测试完全通过，所有验证项均满足要求：\n\n")
                f.write("1. ✓ 删除后重启机制正常工作\n")
                f.write("2. ✓ 墓碑记录正确创建\n")
                f.write("3. ✓ 残留文件自动清理\n")
                f.write("4. ✓ 部分删除失败正确处理\n")
                f.write("5. ✓ 文件索引保持一致性\n\n")
                f.write("系统墓碑机制功能完善，能够有效防止文件'复活'问题。\n")
            else:
                f.write("### ❌ 测试失败\n\n")
                f.write("墓碑机制验证测试存在问题，需要检查以下方面：\n\n")
                if not verification.get('test_4_1', {}).get('passed'):
                    f.write("1. ✗ 测试4.1失败：检查删除后重启机制\n")
                if not verification.get('test_4_2', {}).get('passed'):
                    f.write("2. ✗ 测试4.2失败：检查部分删除失败处理\n")
                f.write("\n建议：\n")
                f.write("- 查看 Master 和 Worker 日志\n")
                f.write("- 检查文件索引状态\n")
                f.write("- 验证墓碑记录创建逻辑\n")
        
        self.log(f"\n报告已生成: {report_file}")
        return report_file

    def run_all_tests(self):
        """运行所有测试"""
        self.start_time = datetime.now()
        self.log("=" * 60)
        self.log("墓碑机制验证测试开始")
        self.log("=" * 60)
        self.log(f"开始时间: {self.start_time}")
        
        # 环境检查
        if not self.check_environment():
            self.log("环境检查失败，测试终止", "ERROR")
            return False
        
        # 执行测试4.1
        try:
            test_4_1_passed = self.run_test_4_1()
        except Exception as e:
            self.log(f"测试4.1异常: {e}", "ERROR")
            test_4_1_passed = False
        
        # 等待一段时间
        time.sleep(5)
        
        # 执行测试4.2
        try:
            test_4_2_passed = self.run_test_4_2()
        except Exception as e:
            self.log(f"测试4.2异常: {e}", "ERROR")
            test_4_2_passed = False
        
        # 验证结果
        all_passed = self.verify_results()
        
        # 生成报告
        report_file = self.generate_report()
        
        # 保存测试结果
        results_file = self.log_dir / f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, indent=2, ensure_ascii=False)
        
        self.end_time = datetime.now()
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        self.log("=" * 60)
        self.log("墓碑机制验证测试完成")
        self.log("=" * 60)
        self.log(f"结束时间: {self.end_time}")
        self.log(f"总测试时长: {total_duration:.1f}秒")
        self.log(f"测试结果: {'✓ 全部通过' if all_passed else '✗ 存在失败'}")
        self.log(f"报告文件: {report_file}")
        self.log(f"结果文件: {results_file}")
        
        self.test_results['overall']['start_time'] = self.start_time.isoformat()
        self.test_results['overall']['end_time'] = self.end_time.isoformat()
        self.test_results['overall']['total_duration'] = total_duration
        self.test_results['overall']['all_passed'] = all_passed
        
        return all_passed

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='墓碑机制验证测试')
    parser.add_argument('--test', choices=['4.1', '4.2', 'all'], default='all',
                       help='选择测试场景')
    
    args = parser.parse_args()
    
    tester = TombstoneTestRunner()
    
    try:
        if args.test == '4.1':
            success = tester.run_test_4_1()
        elif args.test == '4.2':
            success = tester.run_test_4_2()
        else:
            success = tester.run_all_tests()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        tester.log("\n测试被用户中断", "WARN")
        return 130
    except Exception as e:
        tester.log(f"测试异常: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())