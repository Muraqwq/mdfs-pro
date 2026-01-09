#!/usr/bin/env python3
"""
批量上传脚本 - 用于性能测试
支持并发上传、错误重试、性能监控
"""

import os
import sys
import time
import json
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# 默认配置
DEFAULT_MASTER_URL = "http://localhost:8080"
DEFAULT_UPLOAD_ENDPOINT = "/upload"
DEFAULT_ADMIN_SECRET = "admin888"
DEFAULT_CONCURRENT_UPLOADS = 8
DEFAULT_RETRY_COUNT = 3
DEFAULT_TIMEOUT = 300
DEFAULT_CHUNK_SIZE = 8192

# 全局变量
upload_stats = {
    'total_files': 0,
    'success_count': 0,
    'failed_count': 0,
    'retry_count': 0,
    'total_size': 0,
    'uploaded_size': 0,
    'start_time': None,
    'end_time': None,
    'file_results': [],
    'errors': []
}

def get_file_size(file_path):
    """获取文件大小（字节）"""
    return os.path.getsize(file_path)

def format_size(size_bytes):
    """格式化文件大小显示"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def scan_video_files(directory, extensions=None):
    """扫描目录下的所有视频文件"""
    if extensions is None:
        extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v']

    files = []
    directory = Path(directory)

    if not directory.exists():
        raise FileNotFoundError(f"目录不存在: {directory}")

    for ext in extensions:
        for file_path in directory.glob(f"*{ext}"):
            size = get_file_size(file_path)
            files.append({
                'name': file_path.name,
                'path': str(file_path),
                'size': size,
                'status': 'pending',
                'attempts': 0,
                'upload_time': 0,
                'error': None
            })

    return sorted(files, key=lambda x: x['name'])

def upload_file(file_info, master_url, endpoint, admin_secret, timeout, retry_count, chunk_size):
    """单文件上传逻辑"""
    url = f"{master_url}{endpoint}"
    file_path = file_info['path']
    file_name = file_info['name']

    for attempt in range(retry_count):
        try:
            file_info['attempts'] = attempt + 1

            with open(file_path, 'rb') as f:
                files = {'movie': (file_name, f, 'video/mp4')}
                data = {'secret': admin_secret}

                start_time = time.time()

                response = requests.post(
                    url,
                    files=files,
                    data=data,
                    timeout=timeout
                )

                end_time = time.time()
                upload_time = end_time - start_time

                if response.status_code == 200:
                    file_info['status'] = 'success'
                    file_info['upload_time'] = upload_time
                    return {
                        'success': True,
                        'upload_time': upload_time,
                        'attempt': attempt + 1
                    }
                else:
                    raise Exception(f"HTTP {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            error_msg = f"上传超时 (尝试 {attempt + 1}/{retry_count})"
            file_info['error'] = error_msg
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)  # 指数退避
                continue
            else:
                return {
                    'success': False,
                    'error': error_msg,
                    'attempts': attempt + 1
                }

        except requests.exceptions.ConnectionError as e:
            error_msg = f"连接错误: {str(e)}"
            file_info['error'] = error_msg
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                return {
                    'success': False,
                    'error': error_msg,
                    'attempts': attempt + 1
                }

        except Exception as e:
            error_msg = f"上传失败: {str(e)}"
            file_info['error'] = error_msg
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                return {
                    'success': False,
                    'error': error_msg,
                    'attempts': attempt + 1
                }

    return {
        'success': False,
        'error': '超过最大重试次数',
        'attempts': retry_count
    }

def concurrent_upload(files, master_url, endpoint, admin_secret, max_workers, timeout, retry_count, chunk_size):
    """并发上传控制"""
    results = []
    total_size = sum(f['size'] for f in files)
    uploaded_size = 0

    with tqdm(total=len(files), desc="上传进度", unit="文件") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(
                    upload_file,
                    file,
                    master_url,
                    endpoint,
                    admin_secret,
                    timeout,
                    retry_count,
                    chunk_size
                ): file for file in files
            }

            for future in as_completed(future_to_file):
                file = future_to_file[future]
                result = None

                try:
                    result = future.result()
                    file_info = {
                        'name': file['name'],
                        'size': file['size'],
                        'status': 'success' if result['success'] else 'failed',
                        'upload_time': result.get('upload_time', 0),
                        'attempts': result.get('attempts', 0),
                        'error': result.get('error', '')
                    }

                    if result['success']:
                        upload_stats['success_count'] += 1
                        uploaded_size += file['size']
                        pbar.set_postfix({
                            '成功': upload_stats['success_count'],
                            '失败': upload_stats['failed_count'],
                            '已上传': format_size(uploaded_size)
                        })
                    else:
                        upload_stats['failed_count'] += 1
                        upload_stats['errors'].append({
                            'file': file['name'],
                            'error': result.get('error', ''),
                            'attempts': result.get('attempts', 0)
                        })

                    upload_stats['file_results'].append(file_info)
                    pbar.update(1)

                except Exception as e:
                    upload_stats['failed_count'] += 1
                    upload_stats['errors'].append({
                        'file': file['name'],
                        'error': str(e),
                        'attempts': 0
                    })
                    upload_stats['file_results'].append({
                        'name': file['name'],
                        'size': file['size'],
                        'status': 'failed',
                        'upload_time': 0,
                        'attempts': 0,
                        'error': str(e)
                    })
                    pbar.update(1)

    return results

def calculate_performance_metrics():
    """计算性能指标"""
    if not upload_stats['start_time'] or not upload_stats['end_time']:
        return None

    total_time = upload_stats['end_time'] - upload_stats['start_time']
    total_size = upload_stats['total_size']
    success_count = upload_stats['success_count']

    if total_time == 0:
        return None

    # 计算上传速度
    upload_speed = upload_stats['uploaded_size'] / total_time

    # 计算平均上传时间
    success_files = [f for f in upload_stats['file_results'] if f['status'] == 'success']
    if success_files:
        avg_upload_time = sum(f['upload_time'] for f in success_files) / len(success_files)
        max_upload_time = max(f['upload_time'] for f in success_files)
        min_upload_time = min(f['upload_time'] for f in success_files)
    else:
        avg_upload_time = 0
        max_upload_time = 0
        min_upload_time = 0

    # 计算吞吐量
    if total_time > 0 and success_count > 0:
        throughput = success_count / total_time
    else:
        throughput = 0

    return {
        'total_time': total_time,
        'upload_speed': upload_speed,
        'upload_speed_formatted': format_size(upload_speed) + '/s',
        'throughput': throughput,
        'throughput_formatted': f"{throughput:.2f} 文件/秒",
        'avg_upload_time': avg_upload_time,
        'max_upload_time': max_upload_time,
        'min_upload_time': min_upload_time,
        'total_size': total_size,
        'total_size_formatted': format_size(total_size),
        'uploaded_size': upload_stats['uploaded_size'],
        'uploaded_size_formatted': format_size(upload_stats['uploaded_size'])
    }

def generate_report(output_dir, metrics):
    """生成上传报告"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(output_dir, f"upload_report_{timestamp}.md")

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 批量上传测试报告\n\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("## 配置参数\n\n")
        f.write(f"- Master URL: {DEFAULT_MASTER_URL}\n")
        f.write(f"- 并发上传数: {DEFAULT_CONCURRENT_UPLOADS}\n")
        f.write(f"- 失败重试次数: {DEFAULT_RETRY_COUNT}\n")
        f.write(f"- 超时时间: {DEFAULT_TIMEOUT}秒\n\n")

        f.write("## 上传统计\n\n")
        f.write(f"- 总文件数: {upload_stats['total_files']}\n")
        f.write(f"- 成功上传: {upload_stats['success_count']} ({upload_stats['success_count']/upload_stats['total_files']*100:.2f}%)\n")
        f.write(f"- 失败上传: {upload_stats['failed_count']} ({upload_stats['failed_count']/upload_stats['total_files']*100:.2f}%)\n")
        f.write(f"- 重试次数: {upload_stats['retry_count']}\n\n")

        if metrics:
            f.write("## 性能指标\n\n")
            f.write(f"- 总上传时间: {metrics['total_time']:.2f}秒\n")
            f.write(f"- 平均上传速度: {metrics['upload_speed_formatted']}\n")
            f.write(f"- 吞吐量: {metrics['throughput_formatted']}\n")
            f.write(f"- 平均上传时间: {metrics['avg_upload_time']:.2f}秒\n")
            f.write(f"- 最大上传时间: {metrics['max_upload_time']:.2f}秒\n")
            f.write(f"- 最小上传时间: {metrics['min_upload_time']:.2f}秒\n\n")

            f.write("## 数据统计\n\n")
            f.write(f"- 总数据量: {metrics['total_size_formatted']}\n")
            f.write(f"- 已上传数据: {metrics['uploaded_size_formatted']}\n")
            f.write(f"- 上传成功率: {upload_stats['success_count']/upload_stats['total_files']*100:.2f}%\n\n")

        if upload_stats['errors']:
            f.write("## 错误详情\n\n")
            f.write(f"共 {len(upload_stats['errors'])} 个上传失败:\n\n")
            for error in upload_stats['errors'][:20]:  # 只显示前20个错误
                f.write(f"### {error['file']}\n")
                f.write(f"- 错误: {error['error']}\n")
                f.write(f"- 重试次数: {error['attempts']}\n\n")

            if len(upload_stats['errors']) > 20:
                f.write(f"... 还有 {len(upload_stats['errors']) - 20} 个错误未显示\n\n")

        f.write("## 详细文件列表\n\n")
        f.write("| 文件名 | 大小 | 状态 | 上传时间 | 重试次数 | 错误信息 |\n")
        f.write("|--------|------|------|----------|----------|----------|\n")

        for result in upload_stats['file_results']:
            status_icon = "✓" if result['status'] == 'success' else "✗"
            f.write(f"| {result['name']} | {format_size(result['size'])} | {status_icon} {result['status']} | "
                   f"{result['upload_time']:.2f}s | {result['attempts']} | {result.get('error', '')} |\n")

    return report_file

def save_metrics_json(output_dir, metrics):
    """保存性能指标到 JSON 文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_file = os.path.join(output_dir, f"upload_metrics_{timestamp}.json")

    data = {
        'timestamp': datetime.now().isoformat(),
        'config': {
            'master_url': DEFAULT_MASTER_URL,
            'concurrent_uploads': DEFAULT_CONCURRENT_UPLOADS,
            'retry_count': DEFAULT_RETRY_COUNT,
            'timeout': DEFAULT_TIMEOUT
        },
        'stats': {
            'total_files': upload_stats['total_files'],
            'success_count': upload_stats['success_count'],
            'failed_count': upload_stats['failed_count'],
            'total_size': upload_stats['total_size'],
            'uploaded_size': upload_stats['uploaded_size']
        },
        'performance': metrics
    }

    with open(metrics_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return metrics_file

def main():
    parser = argparse.ArgumentParser(description='批量上传脚本 - 分布式存储系统性能测试')
    parser.add_argument('--input_dir', default='../test_videos/split_output',
                       help='包含视频文件的目录 (默认: ../test_videos/split_output)')
    parser.add_argument('--output_dir', default='../test_videos/logs',
                       help='报告输出目录 (默认: ../test_videos/logs)')
    parser.add_argument('--master_url', default=DEFAULT_MASTER_URL,
                       help=f'Master 服务 URL (默认: {DEFAULT_MASTER_URL})')
    parser.add_argument('--concurrent', type=int, default=DEFAULT_CONCURRENT_UPLOADS,
                       help=f'并发上传数量 (默认: {DEFAULT_CONCURRENT_UPLOADS})')
    parser.add_argument('--retry', type=int, default=DEFAULT_RETRY_COUNT,
                       help=f'失败重试次数 (默认: {DEFAULT_RETRY_COUNT})')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT,
                       help=f'单个上传超时时间(秒) (默认: {DEFAULT_TIMEOUT})')
    parser.add_argument('--limit', type=int, default=0,
                       help='限制上传文件数量 (0=全部上传)')
    parser.add_argument('--dry_run', action='store_true',
                       help='只扫描文件，不实际上传')

    args = parser.parse_args()

    print("=" * 60)
    print("批量上传性能测试")
    print("=" * 60)
    print("")
    print("配置参数:")
    print(f"  输入目录: {args.input_dir}")
    print(f"  Master URL: {args.master_url}")
    print(f"  并发上传数: {args.concurrent}")
    print(f"  失败重试次数: {args.retry}")
    print(f"  超时时间: {args.timeout}秒")
    if args.limit > 0:
        print(f"  限制文件数: {args.limit}")
    print("")

    try:
        # 扫描视频文件
        print("正在扫描视频文件...")
        files = scan_video_files(args.input_dir)

        if args.limit > 0:
            files = files[:args.limit]
            print(f"限制上传前 {args.limit} 个文件")

        if not files:
            print("错误: 没有找到可上传的视频文件")
            return 1

        upload_stats['total_files'] = len(files)
        upload_stats['total_size'] = sum(f['size'] for f in files)

        print(f"找到 {len(files)} 个视频文件")
        print(f"总大小: {format_size(upload_stats['total_size'])}")
        print(f"平均大小: {format_size(upload_stats['total_size'] / len(files))}")
        print("")

        if args.dry_run:
            print("== 干运行模式 - 不实际上传 ==")
            print("文件列表:")
            for file in files[:10]:
                print(f"  - {file['name']} ({format_size(file['size'])})")
            if len(files) > 10:
                print(f"  ... 还有 {len(files) - 10} 个文件")
            return 0

        # 执行上传
        print("开始上传...")
        upload_stats['start_time'] = time.time()

        concurrent_upload(
            files,
            args.master_url,
            DEFAULT_UPLOAD_ENDPOINT,
            DEFAULT_ADMIN_SECRET,
            args.concurrent,
            args.timeout,
            args.retry,
            DEFAULT_CHUNK_SIZE
        )

        upload_stats['end_time'] = time.time()
        upload_stats['retry_count'] = sum(f['attempts'] - 1 for f in upload_stats['file_results'])

        print("")
        print("=" * 60)
        print("上传完成")
        print("=" * 60)
        print("")

        # 计算性能指标
        metrics = calculate_performance_metrics()

        if metrics:
            print("性能指标:")
            print(f"  总上传时间: {metrics['total_time']:.2f}秒")
            print(f"  平均上传速度: {metrics['upload_speed_formatted']}")
            print(f"  吞吐量: {metrics['throughput_formatted']}")
            print(f"  平均上传时间: {metrics['avg_upload_time']:.2f}秒")
            print("")

        print("上传统计:")
        print(f"  总文件数: {upload_stats['total_files']}")
        print(f"  成功: {upload_stats['success_count']} ({upload_stats['success_count']/upload_stats['total_files']*100:.1f}%)")
        print(f"  失败: {upload_stats['failed_count']} ({upload_stats['failed_count']/upload_stats['total_files']*100:.1f}%)")
        print(f"  总重试次数: {upload_stats['retry_count']}")
        print("")

        # 生成报告
        os.makedirs(args.output_dir, exist_ok=True)
        report_file = generate_report(args.output_dir, metrics)
        metrics_file = save_metrics_json(args.output_dir, metrics)

        print("报告生成:")
        print(f"  Markdown 报告: {report_file}")
        print(f"  JSON 指标: {metrics_file}")
        print("")

        if upload_stats['failed_count'] > 0:
            print(f"警告: {upload_stats['failed_count']} 个文件上传失败")
            return 1

        return 0

    except FileNotFoundError as e:
        print(f"错误: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n\n用户中断上传")
        return 130
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())