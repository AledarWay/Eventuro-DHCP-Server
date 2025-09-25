import os
import json
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime
from dhcp_server import DHCPServer
from db_manager import DBManager, AuthManager
from telegram_notifier import TelegramNotifier
from web_server import create_app, validate_config

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')

def flatten_config(config):
    # Если конфиг уже плоский (нет секций), возвращаем как есть
    if 'network' not in config:
        return config

    flat_config = {}
    # Разворачиваем секции в плоский формат
    sections = {
        'network': [
            'interface', 'server_ip', 'pool_start', 'pool_end', 'subnet_mask',
            'gateway', 'dns_servers', 'lease_time', 'domain_name'
        ],
        'server': ['cache_ttl', 'expire_check_period'],
        'web': ['web_host', 'web_port', 'web_lease_history_limit'],
        'database': ['db_file', 'auth_db_file', 'history_db_file', 'history_cleanup_days'],
        'logging': ['log_file', 'log_level', 'max_log_size_mb', 'max_log_backup_count'],
        'api': ['api_cache_ttl', 'api_token'],
        'telegram': ['telegram_enabled', 'telegram_notify_new_device', 
                    'telegram_notify_inactive_device', 'inactive_period', 
                    'telegram_bot_token', 'telegram_chat_id', 'telegram_thread_id', 
                    'telegram_web_url', 'telegram_retries', 'telegram_retry_interval']
    }

    for section, keys in sections.items():
        for key in keys:
            if key in config.get(section, {}):
                flat_config[key] = config[section][key]

    return flat_config

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(base_dir, 'config.json')
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logging.error(f"Файл конфигурации не найден: {config_file}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Ошибка парсинга конфигурации {config_file}: {e}")
        raise
    except Exception as e:
        logging.error(f"Ошибка загрузки конфига {config_file}: {e}")
        raise
    
    # Преобразуем конфиг в плоский формат для обратной совместимости
    config = flatten_config(config)

    # Формируем абсолютные пути для файлов баз данных и логов из конфига
    config['db_file'] = os.path.join(base_dir, config.get('db_file', 'dhcp_leases.db'))
    config['auth_db_file'] = os.path.join(base_dir, config.get('auth_db_file', 'web_auth.db'))
    config['history_db_file'] = os.path.join(base_dir, config.get('history_db_file', 'dhcp_lease_history.db'))
    config['log_file'] = os.path.join(base_dir, config.get('log_file', 'dhcp_server.log'))

    # Валидация конфига
    is_valid, error = validate_config(config)
    if not is_valid:
        logging.error(f"Невалидный конфиг: {error}")
        raise ValueError(f"Невалидный конфиг: {error}")

    # Настройка логирования
    formatter = CustomFormatter(
        fmt='%(asctime)s [%(levelname)s] [%(name)s] {%(funcName)s}: %(message)s'
    )
    file_handler = RotatingFileHandler(
        config['log_file'],
        mode='a',
        maxBytes=config['max_log_size_mb']*1024*1024,
        backupCount=config['max_log_backup_count'],
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    
    logger = logging.getLogger()
    logger.setLevel(config.get('log_level', 'INFO'))
    logger.handlers = []
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logging.info("Запуск сервера...")
    
    # Инициализация
    try:
        auth_manager = AuthManager(config['auth_db_file'])
        db_manager = DBManager(config['db_file'], config['history_db_file'], config, None)
        telegram_notifier = TelegramNotifier(config, db_manager)
        db_manager.telegram_notifier = telegram_notifier
        server = DHCPServer(config, db_manager)
        server.start()

        # Запуск веб-сервера
        app = create_app(server, db_manager, auth_manager)
        app.run(host=config['web_host'], port=config['web_port'], debug=False)

    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        logging.info("Перезапуск через 5 секунд...")
        time.sleep(5)
        main()

if __name__ == "__main__":
    main()