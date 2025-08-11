from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
from ..core.task_storage import TaskStorage
from datetime import datetime
import os
import uvicorn


class TaskRequest(BaseModel):
    name: str
    group: str = "default"
    description: str = ""
    task_type: str
    params: Dict[str, Any] = {}

class TaskResponse(BaseModel):
    task_id: str
    message: str

class QTaskServer:
    def __init__(self):
        self.app = FastAPI(title="任务监控系统 API", version="1.0.0")
        self.static_dir = os.path.join(os.path.dirname(__file__), "..", "web","static")
        self._setup_middleware()
        self._setup_static_files()
        self._setup_routes()
    
    def _setup_middleware(self):
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    def _setup_static_files(self):
        if os.path.exists(self.static_dir):
            self.app.mount("/static", StaticFiles(directory=self.static_dir), name="static")
    
    def _setup_routes(self): 
        @self.app.get("/")
        async def serve_frontend():
            """提供前端页面"""
            static_path = os.path.join(self.static_dir, "index.html")
            if os.path.exists(static_path):
                return FileResponse(static_path)
            return {"message": "欢迎使用任务监控系统 API", "docs": "/docs"}

        @self.app.get("/api/stats")
        async def get_stats():
            """获取系统统计信息"""
            storage = TaskStorage()
            retry_stats = storage.get_all_retries()
            
            return {
                'todo_count': storage.redis.llen(storage.queues['TODO']),
                'done_count': storage.redis.scard(storage.queues['DONE']),
                'skip_count': storage.redis.llen(storage.queues['SKIP']),
                'error_count': storage.redis.llen(storage.queues['ERROR']),
                'retry_stats': retry_stats,
                'total_retries': sum(retry_stats.values()) if retry_stats else 0,
                'timestamp': datetime.now().isoformat()
            }

        @self.app.get("/api/tasks")
        async def get_all_tasks():
            """获取所有任务详细信息"""
            storage = TaskStorage()
            return storage.get_all_queues_status()

        @self.app.get("/api/tasks/group/{group_name}")
        async def get_tasks_by_group(group_name: str):
            """获取指定分组的任务"""
            storage = TaskStorage()
            tasks = storage.get_tasks_by_group(group_name)
            
            if not tasks:
                return {"group": group_name, "tasks": {}, "count": 0}
            
            return {
                "group": group_name,
                "tasks": tasks,
                "count": len(tasks)
            }

        @self.app.get("/api/groups")
        async def get_all_groups():
            """获取所有任务分组"""
            storage = TaskStorage()
            groups = storage.get_all_groups()
            
            # 统计每个分组的任务数量
            group_stats = {}
            all_tasks = storage.get_all_task_infos()
            
            for group in groups:
                group_tasks = [task for task in all_tasks.values() if task.get('group', 'default') == group]
                status_counts = {}
                for task in group_tasks:
                    status = task.get('status', 'TODO')
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                group_stats[group] = {
                    'total': len(group_tasks),
                    'status_counts': status_counts
                }
            
            return {
                "groups": groups,
                "group_stats": group_stats
            }

        @self.app.get("/api/tasks/{queue_name}")
        async def get_tasks_by_queue(queue_name: str):
            """获取指定队列的任务信息"""
            storage = TaskStorage()
            detailed_info = storage.get_all_queues_status()
            queue_name_upper = queue_name.upper()
            
            if queue_name_upper not in detailed_info:
                raise HTTPException(
                    status_code=404, 
                    detail=f"队列 {queue_name} 不存在"
                )
            
            return {
                "queue": queue_name_upper,
                "tasks": detailed_info[queue_name_upper],
                "count": len(detailed_info[queue_name_upper]) if isinstance(detailed_info[queue_name_upper], list) else len(detailed_info[queue_name_upper])
            }

        @self.app.post("/api/tasks", response_model=TaskResponse)
        async def create_task(task: TaskRequest):
            """创建新任务"""
            from ..core.task_publisher import TaskPublisher
            
            publisher = TaskPublisher()
            task_id = publisher.publish_named_task(
                name=task.name,
                task_type=task.task_type,
                group=task.group,
                description=task.description,
                **task.params
            )
            
            return TaskResponse(
                task_id=task_id,
                message=f"任务 '{task.name}' 创建成功"
            )

        @self.app.get("/api/dashboard")
        async def get_dashboard_data():
            """获取仪表盘数据"""
            storage = TaskStorage()
            
            # 基础统计
            stats = {
                'todo_count': storage.redis.llen(storage.queues['TODO']),
                'done_count': storage.redis.scard(storage.queues['DONE']),
                'skip_count': storage.redis.llen(storage.queues['SKIP']),
                'error_count': storage.redis.llen(storage.queues['ERROR']),
                'total_retries': sum(storage.get_all_retries().values() or [0])
            }
            
            # 分组统计
            groups = storage.get_all_groups()
            group_stats = {}
            all_tasks = storage.get_all_task_infos()
            
            for group in groups:
                group_tasks = [task for task in all_tasks.values() if task.get('group', 'default') == group]
                status_counts = {'TODO': 0, 'PROCESSING': 0, 'DONE': 0, 'ERROR': 0, 'SKIP': 0}
                
                for task in group_tasks:
                    status = task.get('status', 'TODO')
                    if status in status_counts:
                        status_counts[status] += 1
                
                group_stats[group] = {
                    'total': len(group_tasks),
                    'status_counts': status_counts
                }
            
            # 最近任务
            recent_tasks = []
            for task_id, task_info in list(all_tasks.items())[-10:]:  # 最近10个任务
                recent_tasks.append({
                    'id': task_id,
                    'name': task_info.get('name', '未命名任务'),
                    'group': task_info.get('group', 'default'),
                    'status': task_info.get('status', 'TODO'),
                    'created_time': task_info.get('created_time'),
                    'duration': task_info.get('duration')
                })
            
            return {
                'stats': stats,
                'group_stats': group_stats,
                'recent_tasks': recent_tasks,
                'timestamp': datetime.now().isoformat()
            }

    def run(self, host: str, port: int, reload: bool = False):
        uvicorn.run(self.app, host=host, port=port, reload=reload)


if __name__ == "__main__":
    server = QTaskServer()
    server.run(host="0.0.0.0", port=8000)