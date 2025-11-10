import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger(__name__)

class InfluxDBService:
    _instance = None

    def __new__(cls, config):
        if cls._instance is None:
            cls._instance = super(InfluxDBService, cls).__new__(cls)
            cls._initialize(cls._instance, config)
        return cls._instance

    def _initialize(self, config):
        """Инициализация подключения к InfluxDB"""
        self.enabled = config.get('metrics_enabled', False)
        self.metrics_interval = config.get('metrics_interval', 5)
        if not self.enabled:
            logger.info("Запись метрик в InfluxDB отключена в конфигурации")
            return

        logger.info("Инициализация подключения к InfluxDB...")
        try:
            self.client = InfluxDBClient(
                url=config['url'],
                token=config['token'],
                org=config['org']
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.bucket = config['bucket']
            self.measurement = config['measurement']
            logger.info(f"Успешное подключение к InfluxDB. Бакет: {self.bucket}, Measurement: {self.measurement}")
        except Exception as e:
            logger.error(f"Ошибка инициализации InfluxDB: {str(e)}")
            self.enabled = False
            raise

    def write_dhcp_metrics(self, metrics):
        """Пакетная запись метрик DHCP в InfluxDB"""
        if not self.enabled or not metrics:
            return

        try:
            points = [
                Point(self.measurement)
                .tag("msg_type", msg_type_to_str(msg_type))
                .field("count", count)
                for msg_type, count in metrics.items()
                if count > 0
            ]
            if points:
                self.write_api.write(bucket=self.bucket, record=points)
                logger.debug(f"Записаны метрики DHCP: {[(msg_type_to_str(k), v) for k, v in metrics.items() if v > 0]}")
        except InfluxDBError as e:
            logger.error(f"Ошибка записи метрик в InfluxDB: {e}")
        except Exception as e:
            logger.error(f"Ошибка при записи метрик: {str(e)}")

    def close(self):
        """Закрытие соединения с InfluxDB"""
        if not self.enabled:
            return

        try:
            self.client.close()
            logger.info("Соединение с InfluxDB закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с InfluxDB: {str(e)}")

def msg_type_to_str(msg_type):
    types = {1: 'DISCOVER', 2: 'OFFER', 3: 'REQUEST', 4: 'DECLINE', 5: 'ACK', 6: 'NAK', 7: 'RELEASE', 8: 'INFORM'}
    return types.get(msg_type, f'UNKNOWN({msg_type})')