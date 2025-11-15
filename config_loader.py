import json
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Load and cache configuration from config.json"""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
        return cls._instance
    
    def load(self, config_path: str = None) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        if self._config is not None:
            return self._config
        
        if config_path is None:
            config_path = Path(__file__).parent / "config.json"
        else:
            config_path = Path(config_path)
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            logger.info(f"✓ Configuration loaded from {config_path}")
            return self._config
        except FileNotFoundError:
            logger.error(f"✗ Config file not found: {config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"✗ Invalid JSON in config file: {e}")
            raise
    
    def get_order_type_rules(self, order_type: str) -> Dict[str, Any]:
        """Get validation rules for specific order type"""
        if self._config is None:
            self.load()
        
        return self._config.get("order_types", {}).get(order_type, {})
    
    def get_soql_query(self, query_name: str) -> Dict[str, Any]:
        """Get SOQL query template"""
        if self._config is None:
            self.load()
        
        return self._config.get("salesforce_queries", {}).get(query_name, {})
    
    
    