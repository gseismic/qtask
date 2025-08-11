
import time
from .task_storage import TaskStorage
from .logger import logger

class TaskWorker:
    handlers = {}  # 任务处理器注册表: {task_type: (handler_func, max_retries)}
    
    def __init__(self, task_storage: TaskStorage, max_retries: int = 3):
        self.storage = task_storage
        self.DEFAULT_MAX_RETRIES = max_retries
    
    @classmethod
    def register(cls, task_type, max_retries=None):
        """注册任务处理器装饰器
        
        Args:
            task_type: 任务类型
            max_retries: 该类型任务的最大重试次数，None则使用默认值
        """
        def decorator(func):
            cls.handlers[task_type] = (func, max_retries)
            return func
        return decorator
    
    def process_task(self, task_data):
        """任务处理逻辑"""
        start_time = time.time()
        # 默认有效最大重试次数（当未配置 handler 覆盖时使用）
        effective_max_retries = self.DEFAULT_MAX_RETRIES
        
        try:
            data = task_data['data']
            task_type = data.get('type', 'default') if isinstance(data, dict) else 'default'
            
            # 查找处理器
            handler_info = self.handlers.get(task_type)
            if not handler_info:
                logger.debug(f"No handler for task type: {task_type}, will put back to queue")
                # 没有处理器，放回队列
                return self._create_result('RETRY', None, f"No handler for task type: {task_type}", start_time)
            
            handler_func, handler_max_retries = handler_info
            effective_max_retries = handler_max_retries if handler_max_retries is not None else self.DEFAULT_MAX_RETRIES
            
            logger.info(f"Processing task {task_data['id']} type: {task_type}")
            result = handler_func(data)
            
            # 处理不同的返回值格式
            if isinstance(result, tuple) and len(result) >= 2:
                # 新格式: (status, data, message)
                status, result_data, message = result[0], result[1], result[2] if len(result) > 2 else ""
                if status == 'RETRY':
                    # 记录重试并判断是否达到上限
                    retries = self.storage.increment_retry(task_data['id'])
                    if retries >= effective_max_retries:
                        return self._create_result('ERROR', result_data, f"Max retries reached ({retries}/{effective_max_retries})", start_time)
                    else:
                        return self._create_result('RETRY', result_data, message or f"Will retry ({retries}/{effective_max_retries})", start_time)
                return self._create_result(status, result_data, message, start_time)
            elif isinstance(result, bool):
                # 兼容旧格式: True/False
                status = 'DONE' if result else 'SKIP'
                return self._create_result(status, None, "", start_time)
            else:
                # 其他格式当作失败处理
                return self._create_result('SKIP', None, "Invalid handler return format", start_time)
            
        except Exception as e:
            logger.error(f"Task {task_data['id']} failed: {e}")
            retries = self.storage.increment_retry(task_data['id'])
            # 使用已计算的有效最大重试次数
            status = 'ERROR' if retries >= effective_max_retries else 'RETRY'
            message = f"Exception: {str(e)}"
            return self._create_result(status, None, message, start_time)
    
    def _create_result(self, status, data, message, start_time):
        """创建标准化的处理结果"""
        processing_time = round(time.time() - start_time, 3)
        
        result_data = data or {}
        if isinstance(result_data, dict):
            result_data['metrics'] = {
                'processing_time': processing_time,
                'timestamp': time.time()
            }
        
        return {
            'status': status,
            'data': result_data,
            'message': message,
            'processing_time': processing_time
        }
    
    def run(self):
        """持续处理任务"""
        logger.info("Worker started")
        while True:
            task = self.storage.get_task()
            if task:
                result = self.process_task(task)
                # print('----')
                # print(result)
                # 传递完整的结果信息给storage
                self.storage.handle_result(task['id'], result['status'], result)
            time.sleep(1)

# # 示例任务处理器
# @TaskWorker.register('email', max_retries=2)
# def handle_email(data):
#     """处理邮件任务"""
#     to_email = data.get('params', {}).get('to', 'unknown')
#     logger.info(f"Sending email to: {to_email}")
#     return 'DONE', {'email_id': 'e12345'}, f'Email sent to {to_email}'

# @TaskWorker.register('default')
# def handle_default(data):
#     """默认处理器"""
#     logger.info(f"Processing default task: {data}")
#     if data == 'invalid':
#         return 'SKIP', None, 'Task data is invalid'
#     return 'DONE', {'processed': True}, 'Task completed successfully'