#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from qtask.core.task_worker import TaskWorker
from qtask.core.task_storage import TaskStorage

# 注册自定义任务处理器
@TaskWorker.register('data_processing', max_retries=5)
def handle_data_processing(data):
    """处理数据处理任务"""
    params = data.get('params', {})
    file_path = params.get('file_path', 'unknown')
    operation = params.get('operation', 'unknown')
    
    print(f"Processing file: {file_path} with operation: {operation}")
    
    if operation == 'clean':
        print("Cleaning data...")
        result_data = {
            'processed_file': file_path,
            'records_cleaned': 150,
            'errors_found': 5
        }
        return 'DONE', result_data, f'Successfully cleaned {file_path}'
    elif operation == 'invalid':
        return 'SKIP', None, f'Invalid operation: {operation}'
    else:
        return 'RETRY', None, f'Unknown operation: {operation}, will retry'

@TaskWorker.register('report', max_retries=2)
def handle_report(data):
    """处理报告生成任务"""
    params = data.get('params', {})
    report_type = params.get('report_type', 'unknown')
    month = params.get('month', 'unknown')
    
    print(f"Generating {report_type} report for {month}")
    
    result_data = {
        'report_id': 'RPT_' + month.replace('-', ''),
        'pages': 15,
        'charts': 8
    }
    return 'DONE', result_data, f'Generated {report_type} report for {month}'

@TaskWorker.register('backup', max_retries=1)
def handle_backup(data):
    """处理备份任务"""
    params = data.get('params', {})
    source = params.get('source', 'unknown')
    destination = params.get('destination', 'unknown')
    
    print(f"Backing up from {source} to {destination}")
    
    result_data = {
        'backup_id': 'BK_12345',
        'files_copied': 1250,
        'size_mb': 450.5
    }
    return 'DONE', result_data, f'Backup completed: {source} -> {destination}'

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
