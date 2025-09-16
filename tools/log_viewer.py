#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志查看工具
方便查看和分析日志文件
"""

import sys
from pathlib import Path
import re
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.logging_config import get_current_log_file, list_log_files

def view_log_file(log_file_path, lines=50, filter_pattern=None, show_timestamps=True):
    """
    查看日志文件内容
    
    Args:
        log_file_path: 日志文件路径
        lines: 显示的行数，-1表示显示全部
        filter_pattern: 过滤模式（正则表达式）
        show_timestamps: 是否显示时间戳
    """
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            content = f.readlines()
        
        print(f"📖 查看日志文件: {log_file_path}")
        print(f"📊 总行数: {len(content)}")
        print("=" * 80)
        
        # 过滤内容
        if filter_pattern:
            filtered_content = []
            for line in content:
                if re.search(filter_pattern, line, re.IGNORECASE):
                    filtered_content.append(line)
            content = filtered_content
            print(f"🔍 过滤后行数: {len(content)} (模式: {filter_pattern})")
            print("=" * 80)
        
        # 显示内容
        if lines > 0:
            content = content[-lines:]  # 显示最后几行
        
        for line in content:
            line = line.strip()
            if not line:
                continue
                
            if show_timestamps:
                print(line)
            else:
                # 移除时间戳部分
                # 格式: 2024-12-31 18:35:52 - module - LEVEL - message
                parts = line.split(' - ', 3)
                if len(parts) >= 4:
                    print(f"{parts[3]}")
                else:
                    print(line)
                    
    except FileNotFoundError:
        print(f"❌ 日志文件不存在: {log_file_path}")
    except Exception as e:
        print(f"❌ 读取日志文件失败: {e}")

def search_log(log_file_path, search_term, case_sensitive=False):
    """
    在日志文件中搜索特定内容
    
    Args:
        log_file_path: 日志文件路径
        search_term: 搜索词
        case_sensitive: 是否区分大小写
    """
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"🔍 在日志文件中搜索: {search_term}")
        print(f"📁 文件: {log_file_path}")
        print("=" * 80)
        
        if not case_sensitive:
            search_term = search_term.lower()
            content_lower = content.lower()
            matches = content_lower.count(search_term)
        else:
            matches = content.count(search_term)
        
        print(f"📊 找到 {matches} 个匹配项")
        
        # 显示匹配的行
        lines = content.split('\n')
        for i, line in enumerate(lines, 1):
            if search_term in line if case_sensitive else search_term.lower() in line.lower():
                print(f"第{i}行: {line.strip()}")
                
    except FileNotFoundError:
        print(f"❌ 日志文件不存在: {log_file_path}")
    except Exception as e:
        print(f"❌ 搜索日志文件失败: {e}")

def analyze_log(log_file_path):
    """
    分析日志文件，统计各种信息
    
    Args:
        log_file_path: 日志文件路径
    """
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            content = f.readlines()
        
        print(f"📊 分析日志文件: {log_file_path}")
        print("=" * 80)
        
        # 统计信息
        total_lines = len(content)
        info_count = 0
        warning_count = 0
        error_count = 0
        module_stats = {}
        time_stats = {}
        
        for line in content:
            line = line.strip()
            if not line:
                continue
            
            # 统计日志级别
            if ' - INFO - ' in line:
                info_count += 1
            elif ' - WARNING - ' in line:
                warning_count += 1
            elif ' - ERROR - ' in line:
                error_count += 1
            
            # 统计模块
            parts = line.split(' - ')
            if len(parts) >= 3:
                module = parts[1]
                module_stats[module] = module_stats.get(module, 0) + 1
            
            # 统计时间分布
            if ' - ' in line:
                time_part = line.split(' - ')[0]
                try:
                    hour = datetime.strptime(time_part, '%Y-%m-%d %H:%M:%S').hour
                    time_stats[hour] = time_stats.get(hour, 0) + 1
                except:
                    pass
        
        # 显示统计结果
        print(f"📈 总行数: {total_lines}")
        print(f"ℹ️  INFO: {info_count}")
        print(f"⚠️  WARNING: {warning_count}")
        print(f"❌ ERROR: {error_count}")
        
        print(f"\n🏗️  模块统计:")
        for module, count in sorted(module_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"  {module}: {count}")
        
        print(f"\n⏰ 时间分布:")
        for hour in sorted(time_stats.keys()):
            print(f"  {hour:02d}:00-{hour:02d}:59: {time_stats[hour]}")
            
    except FileNotFoundError:
        print(f"❌ 日志文件不存在: {log_file_path}")
    except Exception as e:
        print(f"❌ 分析日志文件失败: {e}")

def main():
    """主函数"""
    print("🔍 日志查看工具")
    print("=" * 50)
    
    # 获取可用的日志文件
    log_files = list_log_files()
    current_log = get_current_log_file()
    
    if not log_files:
        print("❌ 没有找到日志文件")
        print("💡 请先运行一些测试或启动服务器来生成日志")
        return
    
    print("📁 可用的日志文件:")
    for i, log_file in enumerate(log_files, 1):
        status = " (当前)" if log_file == current_log else ""
        print(f"  {i}. {log_file}{status}")
    
    print("\n🔧 可用命令:")
    print("  1. view <文件路径> [行数] - 查看日志文件")
    print("  2. search <文件路径> <搜索词> - 搜索日志内容")
    print("  3. analyze <文件路径> - 分析日志文件")
    print("  4. tail <文件路径> [行数] - 查看日志尾部")
    print("  5. filter <文件路径> <模式> - 过滤日志内容")
    print("  6. quit - 退出")
    
    while True:
        try:
            command = input("\n请输入命令: ").strip()
            
            if command.lower() in ['quit', 'exit', 'q']:
                print("👋 再见!")
                break
            
            parts = command.split()
            if not parts:
                continue
            
            cmd = parts[0].lower()
            
            if cmd == 'view' and len(parts) >= 2:
                log_file = parts[1]
                lines = int(parts[2]) if len(parts) > 2 else 50
                view_log_file(log_file, lines)
                
            elif cmd == 'search' and len(parts) >= 3:
                log_file = parts[1]
                search_term = ' '.join(parts[2:])
                search_log(log_file, search_term)
                
            elif cmd == 'analyze' and len(parts) >= 2:
                log_file = parts[1]
                analyze_log(log_file)
                
            elif cmd == 'tail' and len(parts) >= 2:
                log_file = parts[1]
                lines = int(parts[2]) if len(parts) > 2 else 20
                view_log_file(log_file, lines)
                
            elif cmd == 'filter' and len(parts) >= 3:
                log_file = parts[1]
                pattern = ' '.join(parts[2:])
                view_log_file(log_file, -1, pattern)
                
            else:
                print("❌ 无效命令，请查看帮助信息")
                
        except KeyboardInterrupt:
            print("\n👋 再见!")
            break
        except Exception as e:
            print(f"❌ 执行命令失败: {e}")

if __name__ == "__main__":
    main() 