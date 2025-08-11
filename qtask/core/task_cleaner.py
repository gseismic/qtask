"""
极简任务清理器 - 直接删除，不归档
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
from .task_storage import TaskStorage


class TaskCleaner:
    """极简任务清理器 - 只删除，不归档"""
    
    def __init__(self, storage: Optional[TaskStorage] = None):
        self.storage = storage or TaskStorage()
    
    def delete_tasks(self, task_ids: List[str]) -> Dict[str, Any]:
        """
        直接删除任务，不做任何归档
        
        Args:
            task_ids: 要删除的任务ID列表
            
        Returns:
            删除结果统计
        """
        if not task_ids:
            return {"success": 0, "failed": 0, "errors": []}
        
        results = {
            "success": 0,
            "failed": 0, 
            "errors": [],
            "timestamp": datetime.now().isoformat()
        }
        
        # 获取所有任务信息（用于验证）
        all_tasks = self.storage.get_all_task_infos()
        
        for task_id in task_ids:
            try:
                if task_id not in all_tasks:
                    results["errors"].append(f"Task {task_id} not found")
                    results["failed"] += 1
                    continue
                
                # 直接删除任务
                self._remove_task_completely(task_id)
                results["success"] += 1
                
            except Exception as e:
                results["errors"].append(f"Task {task_id}: {str(e)}")
                results["failed"] += 1
        
        return results
    
    def _remove_task_completely(self, task_id: str):
        """从Redis中完全移除任务"""
        # 从所有队列中删除
        self._remove_from_all_queues(task_id)
        
        # 删除任务详细信息
        self.storage.redis.hdel(self.storage.task_info_key, task_id)
        
        # 删除重试计数
        self.storage.redis.hdel(self.storage.retries_key, task_id)
    
    def _remove_from_all_queues(self, task_id: str):
        """从所有队列中移除任务"""
        # 从DONE集合中删除
        self.storage.redis.srem(self.storage.queues['DONE'], task_id)
        
        # 从SKIP列表中删除
        self.storage.redis.lrem(self.storage.queues['SKIP'], 0, task_id)
        
        # 从ERROR列表中删除
        self.storage.redis.lrem(self.storage.queues['ERROR'], 0, task_id)
        
        # 从TODO队列中删除（需要特殊处理JSON格式）
        todo_tasks_raw = self.storage.redis.lrange(self.storage.queues['TODO'], 0, -1)
        for task_data in todo_tasks_raw:
            try:
                import json
                task_obj = json.loads(task_data)
                if task_obj.get('id') == task_id:
                    self.storage.redis.lrem(self.storage.queues['TODO'], 1, task_data)
                    break
            except (json.JSONDecodeError, KeyError):
                continue
    
    def preview_delete(self, task_ids: List[str]) -> Dict[str, Any]:
        """预览删除操作 - 显示将要删除的任务信息"""
        if not task_ids:
            return {"total": 0, "found": 0, "not_found": 0, "tasks": []}
        
        all_tasks = self.storage.get_all_task_infos()
        found_tasks = []
        not_found = []
        
        for task_id in task_ids:
            if task_id in all_tasks:
                task_info = all_tasks[task_id]
                found_tasks.append({
                    "id": task_id,
                    "name": task_info.get('name', ''),
                    "group": task_info.get('group', 'default'),
                    "status": task_info.get('status', 'TODO'),
                    "created_time": task_info.get('created_time', ''),
                    "retry_count": self.storage.get_retry_count(task_id)
                })
            else:
                not_found.append(task_id)
        
        return {
            "total": len(task_ids),
            "found": len(found_tasks),
            "not_found": len(not_found),
            "tasks": found_tasks,
            "not_found_ids": not_found
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取简单统计信息"""
        return self.storage.get_statistics()