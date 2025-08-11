import uuid
from .task_storage import TaskStorage
from .logger import logger

class TaskPublisher:
    def __init__(self, task_storage: TaskStorage):
        self.storage = task_storage
    
    def publish(self, task_type: str, name: str = "", data: dict = None, group: str = "default", description: str = "") -> str:
        """发布任务 - 统一接口
        
        Args:
            task_type: 任务类型
            name: 任务名称
            data: 任务数据字典
            group: 任务分组
            description: 任务描述
            
        Returns:
            str: 任务ID
        """
        task_id = str(uuid.uuid4())
        
        # 构造内部任务数据结构
        internal_task_data = {
            'type': task_type,
            'data': data or {}
        }
        
        self.storage.add_task(task_id, internal_task_data, name, group, description)
        logger.info(f"Published task {task_id}: {task_type} - {name}")
        return task_id