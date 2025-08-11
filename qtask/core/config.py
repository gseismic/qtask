import os
import json
import yaml
from typing import Optional, Dict, Any
from pathlib import Path

class QTaskConfig:
    """QTask配置管理类"""
    
    def __init__(self):
        # Redis配置
        self.redis_host = os.getenv('QTASK_REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('QTASK_REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('QTASK_REDIS_DB', '0'))
        self.redis_password = os.getenv('QTASK_REDIS_PASSWORD', None)
        
        # 默认namespace
        self.default_namespace = os.getenv('QTASK_DEFAULT_NAMESPACE', 'default')
        
        # 服务器配置
        self.server_host = os.getenv('QTASK_SERVER_HOST', '127.0.0.1')
        self.server_port = int(os.getenv('QTASK_SERVER_PORT', '8000'))
        
        # 日志配置
        self.log_level = os.getenv('QTASK_LOG_LEVEL', 'INFO')
    
    def get_redis_config(self) -> dict:
        """获取Redis连接配置"""
        config = {
            'host': self.redis_host,
            'port': self.redis_port,
            'db': self.redis_db
        }
        if self.redis_password:
            config['password'] = self.redis_password
        return config
    
    def _load_from_dict(self, config_dict: Dict[str, Any]):
        """从字典加载配置"""
        # Redis配置
        redis_config = config_dict.get('redis', {})
        self.redis_host = redis_config.get('host', self.redis_host)
        self.redis_port = int(redis_config.get('port', self.redis_port))
        self.redis_db = int(redis_config.get('db', self.redis_db))
        self.redis_password = redis_config.get('password', self.redis_password)
        
        # 默认namespace
        self.default_namespace = config_dict.get('default_namespace', self.default_namespace)
        
        # 服务器配置
        server_config = config_dict.get('server', {})
        self.server_host = server_config.get('host', self.server_host)
        self.server_port = int(server_config.get('port', self.server_port))
        
        # 日志配置
        self.log_level = config_dict.get('log_level', self.log_level)
    
    @classmethod
    def from_file(cls, config_file: str) -> 'QTaskConfig':
        """从配置文件创建配置实例"""
        config_path = Path(config_file)
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_file}")
        
        # 创建实例（先从环境变量初始化）
        instance = cls()
        
        # 根据文件扩展名选择解析方式
        suffix = config_path.suffix.lower()
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                if suffix in ['.json']:
                    config_dict = json.load(f)
                elif suffix in ['.yaml', '.yml']:
                    config_dict = yaml.safe_load(f)
                else:
                    raise ValueError(f"不支持的配置文件格式: {suffix}")
            
            # 从字典加载配置（会覆盖环境变量）
            instance._load_from_dict(config_dict)
            
        except Exception as e:
            raise ValueError(f"解析配置文件失败: {e}")
        
        return instance
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'QTaskConfig':
        """从字典创建配置实例"""
        instance = cls()
        instance._load_from_dict(config_dict)
        return instance
    
    def to_dict(self) -> Dict[str, Any]:
        """导出为字典格式"""
        return {
            'redis': {
                'host': self.redis_host,
                'port': self.redis_port,
                'db': self.redis_db,
                'password': self.redis_password
            },
            'default_namespace': self.default_namespace,
            'server': {
                'host': self.server_host,
                'port': self.server_port
            },
            'log_level': self.log_level
        }
    
    def save_to_file(self, config_file: str):
        """保存配置到文件"""
        config_path = Path(config_file)
        suffix = config_path.suffix.lower()
        
        config_dict = self.to_dict()
        
        with open(config_path, 'w', encoding='utf-8') as f:
            if suffix in ['.json']:
                json.dump(config_dict, f, indent=2, ensure_ascii=False)
            elif suffix in ['.yaml', '.yml']:
                yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)
            else:
                raise ValueError(f"不支持的配置文件格式: {suffix}")
