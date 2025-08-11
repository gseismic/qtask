from typing import Dict, Optional
from .config import QTaskConfig
from .task_storage import TaskStorage
from .task_publisher import TaskPublisher

class TaskStorageFactory:
    """TaskStorage和TaskPublisher工厂类"""
    
    def __init__(self, config: Optional[QTaskConfig] = None):
        self.config = config or QTaskConfig()
        self._storage_cache: Dict[str, TaskStorage] = {}
        self._publisher_cache: Dict[str, TaskPublisher] = {}
    
    def get_storage(self, namespace: str = None) -> TaskStorage:
        """获取TaskStorage实例（带缓存）"""
        if namespace is None:
            namespace = self.config.default_namespace
        
        if namespace not in self._storage_cache:
            redis_config = self.config.get_redis_config()
            self._storage_cache[namespace] = TaskStorage(
                namespace=namespace,
                **redis_config
            )
        
        return self._storage_cache[namespace]
    
    def get_publisher(self, namespace: str = None) -> TaskPublisher:
        """获取TaskPublisher实例（带缓存）"""
        if namespace is None:
            namespace = self.config.default_namespace
        
        if namespace not in self._publisher_cache:
            storage = self.get_storage(namespace)
            self._publisher_cache[namespace] = TaskPublisher(storage)
        
        return self._publisher_cache[namespace]
    
    def clear_cache(self):
        """清空缓存（主要用于测试）"""
        self._storage_cache.clear()
        self._publisher_cache.clear()
    
    def get_all_namespaces(self) -> list:
        """获取所有已缓存的namespace"""
        return list(set(self._storage_cache.keys()) | set(self._publisher_cache.keys()))
