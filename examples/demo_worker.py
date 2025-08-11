#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from qtask.core.task_worker import TaskWorker
from qtask.core.task_storage import TaskStorage

# 注册自定义任务处理器
@TaskWorker.register('data_processing')
def handle_data_processing(data):
    """处理数据处理任务"""
    file_path = data['params'].get('file_path', 'unknown')
    operation = data['params'].get('operation', 'unknown')
    print(f"Processing file: {file_path} with operation: {operation}")
    # 模拟数据处理
    if operation == 'clean':
        print("Cleaning data...")
        return True
    return False

@TaskWorker.register('report')
def handle_report(data):
    """处理报告生成任务"""
    report_type = data['params'].get('report_type', 'unknown')
    month = data['params'].get('month', 'unknown')
    print(f"Generating {report_type} report for {month}")
    # 模拟报告生成
    print("Report generated successfully!")
    return True

@TaskWorker.register('backup')
def handle_backup(data):
    """处理备份任务"""
    source = data['params'].get('source', 'unknown')
    destination = data['params'].get('destination', 'unknown')
    print(f"Backing up from {source} to {destination}")
    return True

def main():
    print("Starting demo worker...")
    print(f"Registered handlers: {list(TaskWorker.handlers.keys())}")
    task_storage = TaskStorage()
    worker = TaskWorker(task_storage=task_storage)
    try:
        worker.run()
    except KeyboardInterrupt:
        print("\nWorker stopped by user")

if __name__ == "__main__":
    main()
