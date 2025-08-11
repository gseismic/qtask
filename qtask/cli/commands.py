"""
QTask CLI Commands - Command-line interface for QTask system management.
"""

import click
import logging
from ..core.task_storage import TaskStorage
from ..core.task_publisher import TaskPublisher
from ..core.config import QTaskConfig
from ..core.factory import TaskStorageFactory
from ..api.server import QTaskServer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@click.group()
@click.version_option(version="0.1.0", prog_name="qtask")
def cli():
    """QTask - A Modern Task Queue System"""
    pass


@cli.command()
@click.option('--host', default='127.0.0.1', help='Server host')
@click.option('--port', default=8000, type=int, help='Server port')
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--namespace', default='default', help='Default namespace')
@click.option('--reload', is_flag=True, help='Reload server on code changes')
def server(host: str, port: int, redis_host: str, redis_port: int, redis_db: int, namespace: str, reload: bool):
    """Start QTask web server.
    
    \b
    常用启动示例:
      qtask server                                     # 默认启动 (127.0.0.1:8000)
      qtask server --host 0.0.0.0 --port 8080         # 指定主机和端口
      qtask server --redis-host redis.example.com     # 连接远程Redis
      qtask server --namespace production              # 使用production环境
      qtask server --reload                            # 开发模式，代码变更自动重载
    """
    click.echo(f"Starting QTask server on {host}:{port}")
    click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
    click.echo(f"Namespace: {namespace}")
    
    # 创建配置
    config = QTaskConfig()
    config.server_host = host
    config.server_port = port
    config.redis_host = redis_host
    config.redis_port = redis_port
    config.redis_db = redis_db
    config.default_namespace = namespace
    
    server = QTaskServer(config)
    server.run(host=host, port=port, reload=reload)


@cli.command()
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--namespace', default='default', help='Namespace')
def status(redis_host: str, redis_port: int, redis_db: int, namespace: str):
    """Show system status and statistics."""
    try:
        # 创建配置和工厂
        config = QTaskConfig()
        config.redis_host = redis_host
        config.redis_port = redis_port
        config.redis_db = redis_db
        config.default_namespace = namespace
        
        factory = TaskStorageFactory(config)
        storage = factory.get_storage(namespace)
        stats = storage.get_statistics()
        
        click.echo(f"QTask System Status")
        click.echo("=" * 20)
        click.echo(f"Namespace: {namespace}")
        click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
        click.echo("")
        click.echo(f"TODO tasks: {stats['todo_count']}")
        click.echo(f"DONE tasks: {stats['done_count']}")
        click.echo(f"SKIP tasks: {stats['skip_count']}")
        click.echo(f"ERROR tasks: {stats['error_count']}")
        
        groups = storage.get_all_groups()
        if groups:
            click.echo(f"\nTask Groups: {', '.join(groups)}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.command()
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--namespace', default='default', help='Namespace')
@click.option('--count', default=2, help='Number of demo tasks to create')
def demo(redis_host: str, redis_port: int, redis_db: int, namespace: str, count: int):
    """Run demo: publish sample tasks."""
    try:
        # 创建配置和工厂
        config = QTaskConfig()
        config.redis_host = redis_host
        config.redis_port = redis_port
        config.redis_db = redis_db
        config.default_namespace = namespace
        
        factory = TaskStorageFactory(config)
        publisher = factory.get_publisher(namespace)
        
        click.echo(f"Publishing {count} demo tasks...")
        click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
        click.echo(f"Namespace: {namespace}")
        click.echo("")
        
        demo_tasks = [
            {
                'task_type': 'data_processing',
                'name': 'Process User Data',
                'group': 'batch',
                'description': 'Clean and validate user registration data',
                'data': {'data_source': 'user_registration', 'batch_size': 1000}
            },
            {
                'task_type': 'send_email',
                'name': 'Send Welcome Email',
                'group': 'notification',
                'description': 'Send welcome email to new users',
                'data': {'template': 'welcome', 'target': 'new_users'}
            },
            {
                'task_type': 'generate_report',
                'name': 'Generate Report',
                'group': 'reports',
                'description': 'Generate monthly user activity report',
                'data': {'report_type': 'monthly', 'format': 'pdf'}
            },
            {
                'task_type': 'backup_db',
                'name': 'Backup Database',
                'group': 'misc',
                'description': 'Create daily database backup',
                'data': {'backup_type': 'full', 'compression': 'gzip'}
            }
        ]
        
        # 限制任务数量
        selected_tasks = demo_tasks[:count]
        
        for task in selected_tasks:
            task_id = publisher.publish(
                task_type=task['task_type'],
                name=task['name'],
                data=task['data'],
                group=task['group'],
                description=task['description']
            )
            click.echo(f"✓ Published: {task['name']} (ID: {task_id[:8]}...)")
        
        click.echo(f"\n🎉 Published {len(selected_tasks)} demo tasks!")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)


@cli.command()
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--namespace', default='default', help='Target namespace')
@click.option('--status', help='Query tasks with specific status (TODO|PROCESSING|DONE|ERROR|SKIP)')
@click.option('--group', help='Query tasks in specific group')
@click.option('--task-type', help='Query tasks of specific type')
@click.option('--name-contains', help='Query tasks with name containing specified text')
@click.option('--before', help='Query tasks created before date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
@click.option('--after', help='Query tasks created after date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
@click.option('--older-than', help='Query tasks older than specified time (e.g., 7d, 24h, 30m)')
@click.option('--newer-than', help='Query tasks newer than specified time (e.g., 7d, 24h, 30m)')
@click.option('--todo', is_flag=True, help='Query all TODO tasks')
@click.option('--done', is_flag=True, help='Query all DONE tasks')
@click.option('--error', is_flag=True, help='Query all ERROR tasks')
@click.option('--processing', is_flag=True, help='Query all PROCESSING tasks')
@click.option('--recent', is_flag=True, help='Query tasks from last 24 hours')
@click.option('--format', 'output_format', default='table', type=click.Choice(['table', 'json', 'csv', 'ids']), help='Output format')
@click.option('--limit', type=int, help='Limit number of results')
@click.option('--sort', default='created_time', type=click.Choice(['created_time', 'name', 'status', 'group']), help='Sort by field')
@click.option('--desc', is_flag=True, help='Sort in descending order')
@click.option('--verbose', is_flag=True, help='Show detailed information')
@click.option('--show-data', is_flag=True, help='Show task data content')
@click.option('--count-only', is_flag=True, help='Only show count, not task details')
def query(redis_host: str, redis_port: int, redis_db: int, namespace: str,
          status: str, group: str, task_type: str, name_contains: str, 
          before: str, after: str, older_than: str, newer_than: str,
          todo: bool, done: bool, error: bool, processing: bool, recent: bool,
          output_format: str, limit: int, sort: str, desc: bool, 
          verbose: bool, show_data: bool, count_only: bool):
    """Query tasks based on specified conditions.
    
    \b
    基础查询示例:
      qtask query --todo                              # 查询所有待处理任务
      qtask query --done                              # 查询所有已完成任务
      qtask query --error                             # 查询所有错误任务
      qtask query --processing                        # 查询所有处理中任务
      qtask query --recent                            # 查询最近24小时的任务
    
    \b
    条件查询示例:
      qtask query --group batch                       # 查询指定分组的任务
      qtask query --task-type email                   # 查询指定类型的任务
      qtask query --name-contains "welcome"           # 查询名称包含指定文字的任务
      qtask query --older-than 7d                     # 查询7天前的任务
      qtask query --newer-than 24h                    # 查询最近24小时的任务
      qtask query --before 2024-01-01                 # 查询指定日期前的任务
      qtask query --after 2024-01-01                  # 查询指定日期后的任务
    
    \b
    组合查询示例:
      qtask query --done --group notification         # 查询notification分组的已完成任务
      qtask query --error --newer-than 7d             # 查询最近7天的错误任务
      qtask query --namespace production --processing # 查询production环境的处理中任务
      qtask query --task-type email --done            # 查询email类型的已完成任务
    
    \b
    输出格式示例:
      qtask query --done --format json                # JSON格式输出
      qtask query --done --format csv                 # CSV格式输出
      qtask query --done --format ids                 # 只输出任务ID
      qtask query --done --count-only                 # 只显示数量
      qtask query --done --limit 10                   # 限制显示10条
      qtask query --done --sort name --desc           # 按名称降序排列
      qtask query --done --verbose                    # 显示详细信息
      qtask query --done --show-data                  # 显示任务数据
    """
    try:
        from datetime import datetime, timedelta
        import re
        import json
        
        # 创建配置和工厂
        config = QTaskConfig()
        config.redis_host = redis_host
        config.redis_port = redis_port
        config.redis_db = redis_db
        config.default_namespace = namespace
        
        factory = TaskStorageFactory(config)
        storage = factory.get_storage(namespace)
        
        # 构建查询条件
        conditions = {}
        
        # 处理状态条件
        if todo:
            conditions['statuses'] = 'TODO'
        elif done:
            conditions['statuses'] = 'DONE'
        elif error:
            conditions['statuses'] = 'ERROR'
        elif processing:
            conditions['statuses'] = 'PROCESSING'
        elif status:
            conditions['statuses'] = status
        
        if group:
            conditions['groups'] = group
            
        if task_type:
            conditions['types'] = task_type
        
        # 处理时间条件
        def parse_relative_time(time_str: str) -> datetime:
            match = re.match(r'(\d+)([dhm])', time_str.lower())
            if match:
                value, unit = int(match.group(1)), match.group(2)
                if unit == 'd':
                    return datetime.now() - timedelta(days=value)
                elif unit == 'h':
                    return datetime.now() - timedelta(hours=value)
                elif unit == 'm':
                    return datetime.now() - timedelta(minutes=value)
            raise ValueError(f"无效的时间格式: {time_str}")
        
        def parse_absolute_time(time_str: str) -> datetime:
            if len(time_str) == 10:  # YYYY-MM-DD
                return datetime.strptime(time_str, '%Y-%m-%d')
            else:  # YYYY-MM-DD HH:MM:SS
                return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        
        # 处理各种时间条件
        if recent:
            conditions['after'] = (datetime.now() - timedelta(days=1)).isoformat()
        elif older_than:
            try:
                time_filter = parse_relative_time(older_than)
                conditions['before'] = time_filter.isoformat()
            except ValueError as e:
                click.echo(f"❌ {e}。请使用格式如: 7d, 24h, 30m", err=True)
                return
        elif newer_than:
            try:
                time_filter = parse_relative_time(newer_than)
                conditions['after'] = time_filter.isoformat()
            except ValueError as e:
                click.echo(f"❌ {e}。请使用格式如: 7d, 24h, 30m", err=True)
                return
        elif before:
            try:
                time_filter = parse_absolute_time(before)
                conditions['before'] = time_filter.isoformat()
            except ValueError:
                click.echo(f"❌ 无效的日期格式: {before}。请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS", err=True)
                return
        elif after:
            try:
                time_filter = parse_absolute_time(after)
                conditions['after'] = time_filter.isoformat()
            except ValueError:
                click.echo(f"❌ 无效的日期格式: {after}。请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS", err=True)
                return
        
        # 查询匹配的任务
        from ..core.task_query import TaskQuery
        query_engine = TaskQuery(storage)
        
        if conditions:
            matching_task_ids = query_engine.find_tasks(**conditions)
            # 获取任务详细信息
            all_task_infos = storage.get_all_task_infos()
            matching_tasks = [all_task_infos[task_id] for task_id in matching_task_ids if task_id in all_task_infos]
        else:
            # 如果没有条件，获取所有任务
            all_task_infos = storage.get_all_task_infos()
            matching_tasks = list(all_task_infos.values())
        
        # 名称过滤
        if name_contains:
            matching_tasks = [task for task in matching_tasks 
                            if name_contains.lower() in task.get('name', '').lower()]
        
        # 排序
        def get_sort_key(task):
            if sort == 'created_time':
                return task.get('created_time', '')
            elif sort == 'name':
                return task.get('name', '')
            elif sort == 'status':
                return task.get('status', '')
            elif sort == 'group':
                return task.get('group', '')
            return ''
        
        matching_tasks.sort(key=get_sort_key, reverse=desc)
        
        # 限制结果数量
        if limit:
            matching_tasks = matching_tasks[:limit]
        
        # 只显示数量
        if count_only:
            click.echo(f"匹配任务数量: {len(matching_tasks)}")
            return
        
        if not matching_tasks:
            click.echo("✅ 没有找到匹配的任务")
            return
        
        # 根据输出格式显示结果
        if output_format == 'json':
            _output_json(matching_tasks, conditions, namespace, redis_host, redis_port, redis_db)
        elif output_format == 'csv':
            _output_csv(matching_tasks, verbose, show_data)
        elif output_format == 'ids':
            _output_ids(matching_tasks)
        else:  # table format
            _output_table(matching_tasks, conditions, namespace, redis_host, redis_port, redis_db, verbose, show_data)
            
    except Exception as e:
        click.echo(f"❌ 查询失败: {e}", err=True)


def _output_table(tasks, conditions, namespace, redis_host, redis_port, redis_db, verbose, show_data):
    """表格格式输出"""
    import json
    click.echo(f"=== 任务查询结果 (Namespace: {namespace}) ===")
    click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
    if conditions:
        condition_strs = []
        for key, value in conditions.items():
            if key == 'statuses':
                condition_strs.append(f"状态={value}")
            elif key == 'groups':
                condition_strs.append(f"分组={value}")
            elif key == 'types':
                condition_strs.append(f"类型={value}")
            elif key == 'before':
                condition_strs.append(f"创建时间早于={value[:19]}")
            elif key == 'after':
                condition_strs.append(f"创建时间晚于={value[:19]}")
        if condition_strs:
            click.echo(f"查询条件: {', '.join(condition_strs)}")
    click.echo(f"匹配任务: {len(tasks)}个")
    click.echo("")
    
    if not verbose:
        # 简洁表格格式
        click.echo(f"{'ID':8} {'名称':20} {'类型':15} {'状态':10} {'分组':10} {'创建时间':19}")
        click.echo("-" * 88)
        for task in tasks:
            task_id = task.get('id', 'unknown')[:8]
            task_name = task.get('name', '未命名')[:20]
            task_type_data = task.get('data', {})
            task_type = task_type_data.get('type', 'unknown') if isinstance(task_type_data, dict) else 'unknown'
            task_type = task_type[:15]
            task_status = task.get('status', 'UNKNOWN')[:10]
            task_group = task.get('group', 'default')[:10]
            created_time = task.get('created_time', '')[:19] if task.get('created_time') else 'unknown'
            click.echo(f"{task_id:8} {task_name:20} {task_type:15} {task_status:10} {task_group:10} {created_time:19}")
    else:
        # 详细格式
        for i, task in enumerate(tasks, 1):
            click.echo(f"{i}. {task.get('name', '未命名')} ({task.get('id', 'unknown')[:8]}...)")
            task_data = task.get('data', {})
            task_type = task_data.get('type', 'unknown') if isinstance(task_data, dict) else 'unknown'
            click.echo(f"   类型: {task_type}")
            click.echo(f"   状态: {task.get('status', 'UNKNOWN')}")
            click.echo(f"   分组: {task.get('group', 'default')}")
            click.echo(f"   描述: {task.get('description', '无描述')}")
            click.echo(f"   创建时间: {task.get('created_time', '未知')}")
            if task.get('start_time'):
                click.echo(f"   开始时间: {task.get('start_time')}")
            if task.get('processed_time'):
                click.echo(f"   处理时间: {task.get('processed_time')}")
            if task.get('duration'):
                click.echo(f"   执行时长: {task.get('duration')}秒")
            if show_data and task.get('data'):
                click.echo(f"   任务数据: {json.dumps(task.get('data'), ensure_ascii=False, indent=6)}")
            click.echo("")


def _output_json(tasks, conditions, namespace, redis_host, redis_port, redis_db):
    """JSON格式输出"""
    import json
    result = {
        "query_info": {
            "namespace": namespace,
            "redis": f"{redis_host}:{redis_port} (db: {redis_db})",
            "conditions": conditions,
            "total_count": len(tasks)
        },
        "tasks": tasks
    }
    click.echo(json.dumps(result, ensure_ascii=False, indent=2))


def _output_csv(tasks, verbose, show_data):
    """CSV格式输出"""
    import csv
    import io
    import json
    
    output = io.StringIO()
    
    if verbose or show_data:
        fieldnames = ['id', 'name', 'type', 'status', 'group', 'description', 'created_time', 'start_time', 'processed_time', 'duration', 'data']
    else:
        fieldnames = ['id', 'name', 'type', 'status', 'group', 'created_time']
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for task in tasks:
        row = {}
        for field in fieldnames:
            if field == 'type':
                task_data = task.get('data', {})
                row[field] = task_data.get('type', 'unknown') if isinstance(task_data, dict) else 'unknown'
            elif field == 'data' and show_data:
                row[field] = json.dumps(task.get('data', {}), ensure_ascii=False)
            else:
                row[field] = task.get(field, '')
        writer.writerow(row)
    
    click.echo(output.getvalue().strip())


def _output_ids(tasks):
    """ID列表格式输出"""
    for task in tasks:
        click.echo(task.get('id', 'unknown'))


@cli.command()
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--namespace', default='default', help='Target namespace to clear')
@click.option('--dry-run', is_flag=True, help='Preview mode - show what would be cleared')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def clear(redis_host: str, redis_port: int, redis_db: int, namespace: str, dry_run: bool, force: bool):
    """Clear all tasks in the specified namespace.
    
    \b
    ⚠️  WARNING: This command will delete ALL tasks in the specified namespace!
    
    \b
    使用示例:
      qtask clear --namespace test --dry-run          # 预览test环境的所有任务
      qtask clear --namespace test                    # 清空test环境（需确认）
      qtask clear --namespace test --force            # 强制清空test环境
      qtask clear --namespace production --dry-run    # 预览production环境
      qtask clear --namespace default --force         # 强制清空默认环境
    
    \b
    安全提示:
      • 使用 --dry-run 预览要清空的内容
      • 默认需要确认，使用 --force 跳过确认
      • 此操作不可逆，请谨慎使用
      • 建议先备份重要数据
    """
    try:
        # 创建配置和工厂
        config = QTaskConfig()
        config.redis_host = redis_host
        config.redis_port = redis_port
        config.redis_db = redis_db
        config.default_namespace = namespace
        
        factory = TaskStorageFactory(config)
        storage = factory.get_storage(namespace)
        
        # 获取所有任务信息
        all_task_infos = storage.get_all_task_infos()
        all_tasks = list(all_task_infos.values())
        
        if not all_tasks:
            click.echo(f"✅ Namespace '{namespace}' 已经是空的")
            return
        
        # 获取统计信息
        stats = storage.get_statistics()
        groups = storage.get_all_groups()
        
        # 显示namespace信息
        click.echo(f"=== 清空 Namespace 预览: {namespace} ===")
        click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
        click.echo(f"总任务数量: {len(all_tasks)}")
        click.echo("")
        
        # 显示统计信息
        click.echo("任务状态分布:")
        click.echo(f"  TODO: {stats.get('todo_count', 0)}个")
        click.echo(f"  PROCESSING: {stats.get('processing_count', 0)}个")
        click.echo(f"  DONE: {stats.get('done_count', 0)}个")
        click.echo(f"  ERROR: {stats.get('error_count', 0)}个")
        click.echo(f"  SKIP: {stats.get('skip_count', 0)}个")
        click.echo("")
        
        # 显示分组信息
        if groups:
            click.echo(f"任务分组: {', '.join(groups)} ({len(groups)}个分组)")
        else:
            click.echo("任务分组: 无")
        click.echo("")
        
        # 显示任务类型统计
        type_counts = {}
        for task in all_tasks:
            task_data = task.get('data', {})
            task_type = task_data.get('type', 'unknown') if isinstance(task_data, dict) else 'unknown'
            type_counts[task_type] = type_counts.get(task_type, 0) + 1
        
        if type_counts:
            click.echo("任务类型分布:")
            for task_type, count in sorted(type_counts.items()):
                click.echo(f"  {task_type}: {count}个")
            click.echo("")
        
        # 显示最近的任务示例
        recent_tasks = sorted(all_tasks, key=lambda x: x.get('created_time', ''), reverse=True)[:5]
        click.echo("最近任务示例:")
        for i, task in enumerate(recent_tasks, 1):
            task_id = task.get('id', 'unknown')[:8]
            task_name = task.get('name', '未命名')[:25]
            task_status = task.get('status', 'UNKNOWN')
            task_group = task.get('group', 'default')
            created_time = task.get('created_time', '')[:19] if task.get('created_time') else 'unknown'
            click.echo(f"  {i}. {task_id}... {task_name:25} [{task_status}] {task_group} {created_time}")
        
        if len(all_tasks) > 5:
            click.echo(f"  ... 还有 {len(all_tasks) - 5} 个任务")
        click.echo("")
        
        # Dry-run模式
        if dry_run:
            click.echo("🔍 预览模式 - 使用不带 --dry-run 的命令执行实际清空")
            return
        
        # 安全警告
        click.echo("⚠️  WARNING: 即将删除该namespace中的所有任务！")
        click.echo("⚠️  此操作不可逆，请确认您真的要清空整个namespace！")
        click.echo("")
        
        # 确认删除
        if not force:
            # 需要用户输入namespace名称来确认
            confirm_namespace = click.prompt(f"请输入namespace名称 '{namespace}' 来确认清空操作")
            if confirm_namespace != namespace:
                click.echo("❌ 输入的namespace名称不匹配，取消操作")
                return
                
            if not click.confirm(f"最后确认: 真的要清空namespace '{namespace}' 中的所有 {len(all_tasks)} 个任务吗?"):
                click.echo("取消清空操作")
                return
        
        # 执行清空
        click.echo("正在清空namespace...")
        
        from datetime import datetime
        start_time = datetime.now()
        
        # 使用TaskStorage的clear_namespace方法
        try:
            cleared_count = storage.clear_namespace(namespace)
            success = True
        except Exception as e:
            click.echo(f"❌ 清空失败: {e}")
            return
        
        end_time = datetime.now()
        
        # 显示结果
        click.echo("")
        click.echo("=== 清空完成 ===")
        click.echo(f"✅ 成功清空namespace: {namespace}")
        click.echo(f"✅ 删除任务数量: {cleared_count}个")
        
        duration = (end_time - start_time).total_seconds()
        click.echo(f"⏱️  总耗时: {duration:.2f}秒")
        
    except Exception as e:
        click.echo(f"❌ 清空失败: {e}", err=True)


@cli.command()
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--format', 'output_format', default='table', type=click.Choice(['table', 'list', 'json']), help='Output format')
@click.option('--show-stats', is_flag=True, help='Show statistics for each namespace')
def namespaces(redis_host: str, redis_port: int, redis_db: int, output_format: str, show_stats: bool):
    """List all available namespaces.
    
    \b
    使用示例:
      qtask namespaces                           # 显示所有namespace（表格格式）
      qtask namespaces --format list             # 列表格式显示
      qtask namespaces --format json             # JSON格式显示
      qtask namespaces --show-stats              # 显示每个namespace的统计信息
      qtask namespaces --redis-host remote       # 连接远程Redis查看namespace
    
    \b
    输出格式:
      • table: 表格格式，显示namespace和基本信息
      • list:  简单列表，每行一个namespace
      • json:  JSON格式，便于脚本处理
    """
    try:
        # 创建配置和存储实例
        config = QTaskConfig()
        config.redis_host = redis_host
        config.redis_port = redis_port
        config.redis_db = redis_db
        
        # 使用任意namespace创建storage来扫描所有namespace
        temp_storage = TaskStorage(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            namespace='temp'  # 临时namespace，只用于扫描
        )
        
        # 获取所有namespace
        all_namespaces = temp_storage.get_all_namespaces()
        
        if not all_namespaces:
            click.echo("✅ 没有找到任何namespace")
            return
        
        # 根据输出格式显示结果
        if output_format == 'list':
            _output_namespaces_list(all_namespaces)
        elif output_format == 'json':
            _output_namespaces_json(all_namespaces, redis_host, redis_port, redis_db, show_stats, temp_storage)
        else:  # table format
            _output_namespaces_table(all_namespaces, redis_host, redis_port, redis_db, show_stats, temp_storage)
            
    except Exception as e:
        click.echo(f"❌ 获取namespace列表失败: {e}", err=True)


def _output_namespaces_list(namespaces):
    """列表格式输出namespace"""
    for namespace in sorted(namespaces):
        click.echo(namespace)


def _output_namespaces_json(namespaces, redis_host, redis_port, redis_db, show_stats, temp_storage):
    """JSON格式输出namespace"""
    import json
    
    result = {
        "redis_info": {
            "host": redis_host,
            "port": redis_port,
            "db": redis_db
        },
        "total_count": len(namespaces),
        "namespaces": []
    }
    
    for namespace in sorted(namespaces):
        namespace_info = {"name": namespace}
        
        if show_stats:
            try:
                stats = temp_storage.get_namespace_statistics(namespace)
                namespace_info["statistics"] = stats
            except Exception as e:
                namespace_info["statistics"] = {"error": str(e)}
        
        result["namespaces"].append(namespace_info)
    
    click.echo(json.dumps(result, ensure_ascii=False, indent=2))


def _output_namespaces_table(namespaces, redis_host, redis_port, redis_db, show_stats, temp_storage):
    """表格格式输出namespace"""
    click.echo(f"=== Namespace 列表 ===")
    click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
    click.echo(f"总数量: {len(namespaces)}个")
    click.echo("")
    
    if not show_stats:
        # 简单表格
        click.echo(f"{'Namespace':15} {'状态':8}")
        click.echo("-" * 25)
        for namespace in sorted(namespaces):
            click.echo(f"{namespace:15} {'活跃':8}")
    else:
        # 详细统计表格
        click.echo(f"{'Namespace':15} {'TODO':>6} {'DONE':>6} {'ERROR':>6} {'SKIP':>6} {'总计':>6}")
        click.echo("-" * 65)
        
        total_stats = {'todo_count': 0, 'done_count': 0, 'error_count': 0, 'skip_count': 0, 'total_count': 0}
        
        for namespace in sorted(namespaces):
            try:
                stats = temp_storage.get_namespace_statistics(namespace)
                click.echo(f"{namespace:15} {stats.get('todo_count', 0):>6} {stats.get('done_count', 0):>6} {stats.get('error_count', 0):>6} {stats.get('skip_count', 0):>6} {stats.get('total_count', 0):>6}")
                
                # 累计统计
                for key in total_stats:
                    total_stats[key] += stats.get(key, 0)
                    
            except Exception as e:
                click.echo(f"{namespace:15} {'错误':>6} {'':>6} {'':>6} {'':>6} {'':>6}")
        
        if len(namespaces) > 1:
            click.echo("-" * 65)
            click.echo(f"{'总计':15} {total_stats['todo_count']:>6} {total_stats['done_count']:>6} {total_stats['error_count']:>6} {total_stats['skip_count']:>6} {total_stats['total_count']:>6}")


@cli.command()
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, type=int, help='Redis port')
@click.option('--redis-db', default=0, type=int, help='Redis database')
@click.option('--namespace', default='default', help='Target namespace')
@click.option('--status', help='Clean tasks with specific status (DONE|ERROR|SKIP)')
@click.option('--group', help='Clean tasks in specific group')
@click.option('--task-type', help='Clean tasks of specific type')
@click.option('--older-than', help='Clean tasks older than specified time (e.g., 7d, 24h, 30m)')
@click.option('--before', help='Clean tasks created before date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
@click.option('--done', is_flag=True, help='Clean all completed tasks (DONE status)')
@click.option('--error', is_flag=True, help='Clean all error tasks (ERROR status)')
@click.option('--all-completed', is_flag=True, help='Clean all non-TODO tasks')
@click.option('--dry-run', is_flag=True, help='Preview mode - show what would be deleted')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def clean(redis_host: str, redis_port: int, redis_db: int, namespace: str, 
         status: str, group: str, task_type: str, older_than: str, before: str,
         done: bool, error: bool, all_completed: bool, dry_run: bool, force: bool):
    """Clean tasks based on specified conditions.
    
    \b
    常用清理示例:
      qtask clean --done --dry-run                    # 预览所有已完成任务
      qtask clean --done --force                      # 删除所有已完成任务
      qtask clean --error --group batch               # 删除batch分组的错误任务
      qtask clean --older-than 7d --done              # 删除7天前的已完成任务
      qtask clean --all-completed --older-than 30d    # 删除30天前的所有非TODO任务
      qtask clean --group notification --done         # 删除notification分组的已完成任务
      qtask clean --task-type email --done            # 删除email类型的已完成任务
      qtask clean --before 2024-01-01 --force         # 删除2024年前的所有任务
      qtask clean --namespace production --done       # 清理production环境的已完成任务
    
    \b
    安全提示:
      • 使用 --dry-run 预览要删除的任务
      • 默认需要确认，使用 --force 跳过确认
      • 支持组合多个条件进行精确清理
    """
    try:
        from ..core.task_cleaner import TaskCleaner
        from datetime import datetime, timedelta
        import re
        
        # 创建配置和工厂
        config = QTaskConfig()
        config.redis_host = redis_host
        config.redis_port = redis_port
        config.redis_db = redis_db
        config.default_namespace = namespace
        
        factory = TaskStorageFactory(config)
        storage = factory.get_storage(namespace)
        cleaner = TaskCleaner(storage)
        
        # 构建查询条件
        conditions = {}
        
        # 处理状态条件
        if done:
            conditions['statuses'] = 'DONE'
        elif error:
            conditions['statuses'] = 'ERROR'  
        elif all_completed:
            conditions['statuses'] = 'DONE,ERROR,SKIP'
        elif status:
            conditions['statuses'] = status
            
        if group:
            conditions['groups'] = group
            
        if task_type:
            conditions['types'] = task_type
        
        # 处理时间条件
        time_filter = None
        if older_than:
            # 解析相对时间 (7d, 24h, 30m)
            match = re.match(r'(\d+)([dhm])', older_than.lower())
            if match:
                value, unit = int(match.group(1)), match.group(2)
                if unit == 'd':
                    time_filter = datetime.now() - timedelta(days=value)
                elif unit == 'h':
                    time_filter = datetime.now() - timedelta(hours=value)
                elif unit == 'm':
                    time_filter = datetime.now() - timedelta(minutes=value)
                conditions['before'] = time_filter.isoformat()
            else:
                click.echo(f"❌ 无效的时间格式: {older_than}。请使用格式如: 7d, 24h, 30m", err=True)
                return
                
        elif before:
            # 解析绝对时间
            try:
                if len(before) == 10:  # YYYY-MM-DD
                    time_filter = datetime.strptime(before, '%Y-%m-%d')
                else:  # YYYY-MM-DD HH:MM:SS
                    time_filter = datetime.strptime(before, '%Y-%m-%d %H:%M:%S')
                conditions['before'] = time_filter.isoformat()
            except ValueError:
                click.echo(f"❌ 无效的日期格式: {before}。请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS", err=True)
                return
        
        # 查询匹配的任务
        from ..core.task_query import TaskQuery
        query = TaskQuery(storage)
        
        if conditions:
            matching_task_ids = query.find_tasks(**conditions)
            # 获取任务详细信息
            all_task_infos = storage.get_all_task_infos()
            matching_tasks = [all_task_infos[task_id] for task_id in matching_task_ids if task_id in all_task_infos]
        else:
            click.echo("❌ 请指定至少一个清理条件", err=True)
            return
        
        if not matching_tasks:
            click.echo("✅ 没有找到匹配的任务")
            return
        
        # 显示匹配的任务
        click.echo(f"=== 清理任务预览 (Namespace: {namespace}) ===")
        click.echo(f"Redis: {redis_host}:{redis_port} (db: {redis_db})")
        click.echo(f"匹配任务数量: {len(matching_tasks)}")
        click.echo("")
        
        # 按状态分组统计
        status_counts = {}
        for task in matching_tasks:
            task_status = task.get('status', 'UNKNOWN')
            status_counts[task_status] = status_counts.get(task_status, 0) + 1
        
        click.echo("任务状态分布:")
        for status_name, count in status_counts.items():
            click.echo(f"  {status_name}: {count}个")
        click.echo("")
        
        # 显示任务列表预览（全部显示）
        click.echo("任务预览:")
        for i, task in enumerate(matching_tasks):
            task_id = task.get('id', 'unknown')[:8]
            task_name = task.get('name', '未命名')[:20]
            task_status = task.get('status', 'UNKNOWN')
            task_group = task.get('group', 'default')
            created_time = task.get('created_time', '')[:19] if task.get('created_time') else 'unknown'
            click.echo(f"  {i+1:3d}. {task_id}... {task_name:20} [{task_status}] {task_group} {created_time}")
        click.echo("")
        
        # Dry-run模式
        if dry_run:
            click.echo("🔍 预览模式 - 使用 --force 参数执行实际删除")
            return
        
        # 确认删除
        if not force:
            if not click.confirm(f"确认删除这 {len(matching_tasks)} 个任务?"):
                click.echo("取消删除")
                return
        
        # 执行删除
        click.echo("正在删除任务...")
        task_ids = matching_task_ids
        
        start_time = datetime.now()
        result = cleaner.delete_tasks(task_ids)
        end_time = datetime.now()
        
        # 显示结果
        click.echo("")
        click.echo("=== 清理完成 ===")
        click.echo(f"✅ 成功删除: {result['success']}个")
        if result['failed'] > 0:
            click.echo(f"❌ 删除失败: {result['failed']}个")
            for error in result['errors']:
                click.echo(f"   错误: {error}")
        
        duration = (end_time - start_time).total_seconds()
        click.echo(f"⏱️  总耗时: {duration:.2f}秒")
        
    except Exception as e:
        click.echo(f"❌ 清理失败: {e}", err=True)


if __name__ == '__main__':
    cli()
