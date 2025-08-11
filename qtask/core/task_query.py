"""
极简任务查询器 - 只做查询
"""
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re
from .task_storage import TaskStorage


class TaskQuery:
    """极简任务查询器"""
    
    def __init__(self, storage: Optional[TaskStorage] = None):
        self.storage = storage or TaskStorage()
    
    def find_tasks(self, **filters) -> List[str]:
        """
        查询任务，返回任务ID列表
        
        支持的过滤条件:
        - types: str                     # 任务类型 (逗号分隔)
        - groups: str                    # 任务分组 (逗号分隔)
        - statuses: str                  # 任务状态 (逗号分隔)
        - before: str                    # 创建时间之前
        - after: str                     # 创建时间之后
        - name_contains: str             # 名称包含
        """
        all_tasks = self.storage.get_all_task_infos()
        matched_ids = []

        print('--------------------------------')
        print('**all_tasks', all_tasks)
        print('--------------------------------')
        
        # 标准化过滤条件
        normalized_filters = self._normalize_filters(filters)
        
        for task_id, task_info in all_tasks.items():
            if self._match_filters(task_info, normalized_filters):
                matched_ids.append(task_id)
        
        return matched_ids
    
    def get_task_details(self, task_ids: List[str]) -> List[Dict[str, Any]]:
        """获取任务详细信息用于展示"""
        all_tasks = self.storage.get_all_task_infos()
        details = []
        
        for task_id in task_ids:
            if task_id in all_tasks:
                task_info = all_tasks[task_id]
                # 添加重试次数
                retry_count = self.storage.get_retry_count(task_id)
                task_info['retry_count'] = retry_count
                details.append(task_info)
        
        return details
    
    def _normalize_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """标准化过滤条件"""
        normalized = {}
        
        for key, value in filters.items():
            if value is None or value == "":
                continue
                
            if key in ['types', 'groups', 'statuses']:
                # 转换为列表
                normalized[key] = [v.strip() for v in value.split(',') if v.strip()]
            elif key in ['before', 'after']:
                # 解析时间
                parsed_time = self._parse_time(value)
                if parsed_time:
                    normalized[key] = parsed_time
            else:
                normalized[key] = value
        
        return normalized
    
    def _parse_time(self, time_str: str) -> Optional[datetime]:
        """解析时间字符串"""
        if not time_str:
            return None
            
        # 相对时间格式: "7 days ago"
        relative_pattern = r'(\d+)\s+(day|days|hour|hours)\s+ago'
        match = re.match(relative_pattern, time_str.lower())
        
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            
            if 'day' in unit:
                return datetime.now() - timedelta(days=amount)
            elif 'hour' in unit:
                return datetime.now() - timedelta(hours=amount)
        
        # 简单日期格式
        try:
            return datetime.strptime(time_str, '%Y-%m-%d')
        except ValueError:
            try:
                return datetime.fromisoformat(time_str)
            except ValueError:
                return None
    
    def _match_filters(self, task_info: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """检查任务是否匹配过滤条件"""
        for key, value in filters.items():
            if not self._check_condition(task_info, key, value):
                return False
        return True
    
    def _check_condition(self, task_info: Dict[str, Any], key: str, value: Any) -> bool:
        """检查单个条件"""
        if key == 'types':
            task_data = task_info.get('data', {})
            if isinstance(task_data, dict):
                task_type = task_data.get('type', 'default')
            else:
                task_type = 'default'
            return task_type in value
            
        elif key == 'groups':
            return task_info.get('group', 'default') in value
            
        elif key == 'statuses':
            return task_info.get('status', 'TODO') in value
            
        elif key == 'before':
            created_time = task_info.get('created_time')
            if not created_time:
                return False
            try:
                task_time = datetime.fromisoformat(created_time)
                return task_time < value
            except ValueError:
                return False
            
        elif key == 'after':
            created_time = task_info.get('created_time')
            if not created_time:
                return False
            try:
                task_time = datetime.fromisoformat(created_time)
                return task_time > value
            except ValueError:
                return False
            
        elif key == 'name_contains':
            name = task_info.get('name', '')
            return value.lower() in name.lower()
        
        return True