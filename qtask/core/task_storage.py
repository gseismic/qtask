import redis
import json
from datetime import datetime

class TaskStorage:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.redis = redis.Redis(host=host, port=port, db=db, password=password)
        self.queues = {
            'TODO': 'queue:todo',
            'DONE': 'set:done',
            'NULL': 'list:null',
            'ERROR': 'list:error'
        }
        self.retries_key = 'hash:task_retries'
        self.task_info_key = 'hash:task_info'  # 存储任务详细信息和时间
    
    # --- 核心方法 ---
    def add_task(self, task_id, task_data, name="", group="default", description=""):
        """添加新任务到TODO队列"""
        # 保存任务到队列
        self.redis.lpush(self.queues['TODO'], 
                         json.dumps({'id': task_id, 'data': task_data}))
        # 保存任务详细信息
        task_info = {
            'id': task_id,
            'name': name,
            'group': group,
            'description': description,
            'data': task_data,
            'created_time': datetime.now().isoformat(),
            'status': 'TODO',
            'start_time': None,
            'end_time': None,
            'duration': None
        }
        self.redis.hset(self.task_info_key, task_id, json.dumps(task_info))
    
    def get_task(self):
        """从TODO队列获取任务"""
        task_data = self.redis.rpop(self.queues['TODO'])
        if task_data:
            task = json.loads(task_data)
            # 更新任务开始时间
            self.update_task_start_time(task['id'])
            return task
        return None
    
    def update_task_start_time(self, task_id):
        """更新任务开始处理时间"""
        task_info_str = self.redis.hget(self.task_info_key, task_id)
        if task_info_str:
            task_info = json.loads(task_info_str)
            task_info['start_time'] = datetime.now().isoformat()
            task_info['status'] = 'PROCESSING'
            self.redis.hset(self.task_info_key, task_id, json.dumps(task_info))
    
    def handle_result(self, task_id, result_type):
        """根据处理结果转移任务位置"""
        # 更新任务完成信息
        self.update_task_end_time(task_id, result_type)
        
        if result_type == 'DONE':
            self.redis.sadd(self.queues['DONE'], task_id)
        elif result_type == 'NULL':
            self.redis.lpush(self.queues['NULL'], task_id)
        elif result_type == 'ERROR':
            self.redis.lpush(self.queues['ERROR'], task_id)
        elif result_type == 'FAIL':  # 重试情况，放回TODO队列
            task_info_str = self.redis.hget(self.task_info_key, task_id)
            if task_info_str:
                task_info = json.loads(task_info_str)
                self.redis.lpush(self.queues['TODO'], 
                               json.dumps({'id': task_id, 'data': task_info['data']}))
    
    def update_task_end_time(self, task_id, result_type):
        """更新任务完成时间和计算处理时长"""
        task_info_str = self.redis.hget(self.task_info_key, task_id)
        if task_info_str:
            task_info = json.loads(task_info_str)
            end_time = datetime.now()
            task_info['end_time'] = end_time.isoformat()
            task_info['status'] = result_type
            
            # 计算处理时长
            if task_info['start_time']:
                start_time = datetime.fromisoformat(task_info['start_time'])
                duration = (end_time - start_time).total_seconds()
                task_info['duration'] = round(duration, 2)
            
            self.redis.hset(self.task_info_key, task_id, json.dumps(task_info))
    
    def increment_retry(self, task_id):
        """增加任务重试计数"""
        retries = self.redis.hincrby(self.retries_key, task_id, 1)
        return int(retries)
    
    def get_retry_count(self, task_id):
        """获取任务重试次数"""
        return int(self.redis.hget(self.retries_key, task_id) or 0)
    
    # --- 新增队列获取方法 ---
    def get_all_todo_tasks(self):
        """获取TODO队列所有任务（解析后）"""
        raw_tasks = self.redis.lrange(self.queues['TODO'], 0, -1)
        return [json.loads(task.decode('utf-8')) for task in raw_tasks]
    
    def get_all_done_tasks(self):
        """获取DONE集合所有任务ID（解析后）"""
        raw_tasks = self.redis.smembers(self.queues['DONE'])
        return [task_id.decode('utf-8') for task_id in raw_tasks]
    
    def get_all_null_tasks(self):
        """获取NULL队列所有任务ID（解析后）"""
        raw_tasks = self.redis.lrange(self.queues['NULL'], 0, -1)
        return [task_id.decode('utf-8') for task_id in raw_tasks]
    
    def get_all_error_tasks(self):
        """获取ERROR队列所有任务ID（解析后）"""
        raw_tasks = self.redis.lrange(self.queues['ERROR'], 0, -1)
        return [task_id.decode('utf-8') for task_id in raw_tasks]
    
    def get_all_retries(self):
        """获取所有任务重试次数（解析后）"""
        raw_retries = self.redis.hgetall(self.retries_key)
        return {
            task_id.decode('utf-8'): int(retry_count) 
            for task_id, retry_count in raw_retries.items()
        }
    
    def get_task_info(self, task_id):
        """获取单个任务详细信息"""
        task_info_str = self.redis.hget(self.task_info_key, task_id)
        return json.loads(task_info_str) if task_info_str else None
    
    def get_all_task_infos(self):
        """获取所有任务详细信息"""
        raw_infos = self.redis.hgetall(self.task_info_key)
        return {
            task_id.decode('utf-8'): json.loads(task_info) 
            for task_id, task_info in raw_infos.items()
        }
    
    def get_tasks_by_group(self, group_name):
        """获取指定分组的任务"""
        all_infos = self.get_all_task_infos()
        return {
            task_id: task_info 
            for task_id, task_info in all_infos.items() 
            if task_info.get('group', 'default') == group_name
        }
    
    def get_all_groups(self):
        """获取所有任务分组"""
        all_infos = self.get_all_task_infos()
        groups = set()
        for task_info in all_infos.values():
            groups.add(task_info.get('group', 'default'))
        return list(groups)

    # --- 统一获取接口 ---
    def get_all_queues_status(self):
        """获取所有队列状态（解析后）"""
        return {
            'TODO': self.get_all_todo_tasks(),
            'DONE': self.get_all_done_tasks(),
            'NULL': self.get_all_null_tasks(),
            'ERROR': self.get_all_error_tasks(),
            'RETRIES': self.get_all_retries(),
            'TASK_INFOS': self.get_all_task_infos()
        }
    
    def get_statistics(self):
        """获取系统统计信息"""
        return {
            'todo_count': self.redis.llen(self.queues['TODO']),
            'done_count': self.redis.scard(self.queues['DONE']),
            'null_count': self.redis.llen(self.queues['NULL']),
            'error_count': self.redis.llen(self.queues['ERROR']),
            'total_count': self.redis.llen(self.queues['TODO']) + self.redis.scard(self.queues['DONE']) + self.redis.llen(self.queues['NULL']) + self.redis.llen(self.queues['ERROR'])
        }