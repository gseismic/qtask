#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from qtask.core.task_publisher import TaskPublisher
from qtask.core.task_storage import TaskStorage

def main():
    task_storage = TaskStorage()
    publisher = TaskPublisher(task_storage=task_storage)
    
    # 发布不同类型的任务
    print("Publishing tasks...")
    
    # 邮件任务
    task_id1 = publisher.publish(
        task_type='email',
        name="Send Welcome Email",
        data={
            'to': 'user@example.com',
            'subject': 'Welcome!',
            'body': 'Welcome to our service!'
        },
        group="notification",
        description="Send welcome email to new user"
    )
    print(f"Published email task: {task_id1}")
    
    # 数据处理任务
    task_id2 = publisher.publish(
        task_type='data_processing',
        name="Process CSV Data",
        data={
            'file_path': '/tmp/data.csv',
            'operation': 'clean'
        },
        group="batch",
        description="Process uploaded CSV file"
    )
    print(f"Published data processing task: {task_id2}")
    
    # 报告生成任务
    task_id3 = publisher.publish(
        task_type='report',
        name="Generate Monthly Report",
        data={
            'report_type': 'monthly',
            'month': '2024-01'
        },
        group="reports",
        description="Generate monthly sales report"
    )
    print(f"Published report task: {task_id3}")
    
    # 简单任务（无数据）
    task_id4 = publisher.publish(
        task_type="simple",
        name="Simple Task",
        group="misc",
        description="A simple task without specific data"
    )
    print(f"Published simple task: {task_id4}")
    
    print("All tasks published successfully!")

if __name__ == "__main__":
    main()
