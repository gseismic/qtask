"""
QTask CLI Commands - Command-line interface for QTask system management.
"""

import click
import logging
from ..core.task_storage import TaskStorage
from ..core.task_publisher import TaskPublisher
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
@click.option('--host', default='0.0.0.0', help='Server host')
@click.option('--port', default=8000, help='Server port')
@click.option('--reload', is_flag=True, help='Reload server on code changes')
def server(host: str, port: int, reload: bool):
    """Start QTask web server."""
    click.echo(f"Starting QTask server on {host}:{port}")
    
    server = QTaskServer()
    server.run(host=host, port=port, reload=reload)


@cli.command()
def status():
    """Show system status and statistics."""
    try:
        storage = TaskStorage()
        stats = storage.get_statistics()
        
        click.echo("QTask System Status")
        click.echo("=" * 20)
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
def demo():
    """Run demo: publish sample tasks."""
    click.echo("Publishing demo tasks...")
    
    try:
        publisher = TaskPublisher()
        
        demo_tasks = [
            {
                'name': 'Process User Data',
                'type': 'data_processing',
                'group': 'data_processing',
                'description': 'Clean and validate user registration data',
                'params': {'data_source': 'user_registration', 'batch_size': 1000}
            },
            {
                'name': 'Send Welcome Email',
                'type': 'send_email',
                'group': 'email_service',
                'description': 'Send welcome email to new users',
                'params': {'template': 'welcome', 'target': 'new_users'}
            }
        ]
        
        for task in demo_tasks:
            task_id = publisher.publish_named_task(
                name=task['name'],
                task_type=task['type'],
                group=task['group'],
                description=task['description'],
                **task['params']
            )
            click.echo(f"✓ Published: {task['name']} (ID: {task_id[:8]}...)")
        
        click.echo(f"\nPublished {len(demo_tasks)} demo tasks!")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


if __name__ == '__main__':
    cli()
