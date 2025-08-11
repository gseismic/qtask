
import time
from .task_storage import TaskStorage
from .logger import logger

class TaskWorker:
    handlers = {}  # 任务处理器注册表
    
    def __init__(self, task_storage: TaskStorage, max_retries: int = 3):
        self.storage = task_storage
        self.MAX_RETRIES = max_retries
    
    @classmethod
    def register(cls, task_type):
        """注册任务处理器装饰器"""
        def decorator(func):
            cls.handlers[task_type] = func
            return func
        return decorator
    
    def process_task(self, task_data):
        """任务处理逻辑
        
        1. 获取任务数据
        2. 根据任务类型获取处理器
        3. 执行处理器
        4. 处理结果
        5. 返回结果
        6. 如果失败，重试
        """
        try:
            data = task_data['data']
            task_type = data.get('type', 'default') if isinstance(data, dict) else 'default'
            
            # 查找处理器 
            handler = self.handlers.get(task_type) 
            if not handler: 
                logger.warning(f"No handler for task type: {task_type}") 
                return 'NULL'
            
            logger.info(f"Processing task {task_data['id']} type: {task_type}")
            result = handler(data)
            return 'DONE' if result else 'NULL'
            
        except Exception as e:
            logger.error(f"Task {task_data['id']} failed: {e}")
            retries = self.storage.increment_retry(task_data['id'])
            return 'ERROR' if retries >= self.MAX_RETRIES else 'FAIL'
    
    def run(self):
        """持续处理任务"""
        logger.info("Worker started")
        while True:
            task = self.storage.get_task() 
            if task: 
                result = self.process_task(task) 
                self.storage.handle_result(task['id'], result) 
            time.sleep(1)

# 示例任务处理器
@TaskWorker.register('email')
def handle_email(data):
    """处理邮件任务"""
    logger.info(f"Sending email to: {data.get('to', 'unknown')}")
    return True

@TaskWorker.register('default')
def handle_default(data):
    """默认处理器"""
    logger.info(f"Processing default task: {data}")
    return data != 'invalid'