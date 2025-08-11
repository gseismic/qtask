import uuid
from .task_storage import TaskStorage
from .logger import logger

class TaskPublisher:
    def __init__(self, task_storage: TaskStorage):
        self.storage = task_storage
    
    def publish(self, task_data, name="", group="default", description=""):
        """发布新任务"""
        task_id = str(uuid.uuid4())
        self.storage.add_task(task_id, task_data, name, group, description)
        logger.info(f"Published task {task_id}: {task_data}")
        return task_id
    
    def publish_with_params(self, task_type, name="", group="default", description="", **params):
        """发布带详细参数的任务"""
        task_data = {
            'type': task_type,
            'params': params
        }
        return self.publish(task_data, name, group, description)
    
    def publish_named_task(self, name, task_type, group="default", description="", **params):
        """发布命名任务 - 更友好的接口"""
        return self.publish_with_params(
            task_type=task_type,
            name=name,
            group=group, 
            description=description,
            **params
        )