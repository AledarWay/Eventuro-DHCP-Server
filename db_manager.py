import sqlite3
import datetime
import logging
import socket
import struct
import time
from datetime import datetime, timedelta

class DBManager:
    def __init__(self, db_file, history_db_file, config, telegram_notifier):
        self.db_file = db_file
        self.history_db_file = history_db_file
        self.config = config
        self.telegram_notifier = telegram_notifier
        self.history_cleanup_days = config.get('history_cleanup_days', 0)
        self.init_db()
        self.init_history_db()
        self.check_subnet_consistency()

    def get_connection(self):
        conn = sqlite3.connect(self.db_file, check_same_thread=False, timeout=10)
        return conn

    def get_history_connection(self):
        conn = sqlite3.connect(self.history_db_file, check_same_thread=False, timeout=10)
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS leases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT,
                    mac TEXT UNIQUE,
                    hostname TEXT,
                    ip TEXT UNIQUE,
                    lease_type TEXT NOT NULL,
                    expire_at TEXT,
                    is_expired INTEGER NOT NULL,
                    updated_at TEXT,
                    created_at TEXT NOT NULL,
                    create_channel TEXT NOT NULL CHECK (create_channel IN ('DHCP_REQUEST', 'STATIC_LEASE')),
                    deleted_at TEXT,
                    is_blocked INTEGER NOT NULL DEFAULT 0,
                    is_custom_hostname INTEGER DEFAULT 0,
                    trust_flag INTEGER NOT NULL DEFAULT 0
                )
            ''')
            conn.commit()
        logging.info("Инициализирована основная база данных SQLite.")

    def init_history_db(self):
        with self.get_history_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS lease_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mac TEXT NOT NULL,
                    action TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    description TEXT,
                    ip TEXT,
                    new_ip TEXT,
                    name TEXT,
                    new_name TEXT,
                    client_id TEXT,
                    change_channel TEXT NOT NULL CHECK (change_channel IN ('WEB', 'DHCP'))
                )
            ''')
            conn.commit()
        logging.info("Инициализирована база данных истории SQLite.")

    def clean_old_history(self):
        if self.history_cleanup_days == 0:
            logging.debug("Очистка истории отключена (history_cleanup_days=0).")
            return

        try:
            with self.get_history_connection() as conn:
                cursor = conn.cursor()
                cleanup_threshold = (datetime.now() - timedelta(days=self.history_cleanup_days)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                # Очищаем только самые часты и повторяющиеся LEASE_RENEWED и INFORM
                cursor.execute("""
                    DELETE FROM lease_history
                    WHERE action IN ('LEASE_RENEWED', 'INFORM')
                    AND timestamp < ?
                """, (cleanup_threshold,))
                deleted_rows = cursor.rowcount
                conn.commit()
                
                if deleted_rows > 0:
                    logging.info(f"Удалено {deleted_rows} устаревших записей истории (LEASE_RENEWED, INFORM) старше {self.history_cleanup_days} дней.")
                else:
                    logging.debug(f"Устаревшие записи (LEASE_RENEWED, INFORM) старше {self.history_cleanup_days} дней не найдены.")
        except Exception as e:
            logging.error(f"Ошибка при очистке устаревших записей истории: {e}")

    def check_subnet_consistency(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT mac, ip FROM leases WHERE ip IS NOT NULL AND deleted_at IS NULL")
            rows = cursor.fetchall()
            inconsistent = False
            for mac, ip in rows:
                if not self._is_in_subnet(ip):
                    inconsistent = True
                    break
            if inconsistent:
                logging.warning("Обнаружено несоответствие подсети. Начинается миграция...")
                self.migrate_subnet()

    def _is_in_subnet(self, ip):
        try:
            if not ip:
                return False
            mask_int = self.ip_to_int(self.config['subnet_mask'])
            server_int = self.ip_to_int(self.config['server_ip'])
            network_int = server_int & mask_int
            ip_int = self.ip_to_int(ip)
            return (ip_int & mask_int) == network_int
        except Exception as e:
            logging.error(f"Ошибка при проверке подсети для IP {ip}: {e}")
            return False

    def migrate_subnet(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            mask_int = self.ip_to_int(self.config['subnet_mask'])
            new_network_int = self.ip_to_int(self.config['server_ip']) & mask_int
            pool_start_int = self.ip_to_int(self.config['pool_start'])
            pool_end_int = self.ip_to_int(self.config['pool_end'])
            cursor.execute("SELECT mac, ip, hostname, client_id FROM leases WHERE ip IS NOT NULL")
            rows = cursor.fetchall()
            for mac, old_ip, hostname, client_id in rows:
                try:
                    old_ip_int = self.ip_to_int(old_ip)
                    host_part = old_ip_int & ~mask_int
                    new_ip_int = new_network_int | host_part
                    new_ip = self.int_to_ip(new_ip_int)
                    if pool_start_int <= new_ip_int <= pool_end_int:
                        cursor.execute("SELECT 1 FROM leases WHERE ip = ? AND deleted_at IS NULL", (new_ip,))
                        if not cursor.fetchone():
                            cursor.execute("UPDATE leases SET ip = ? WHERE mac = ?", (new_ip, mac))
                            self._insert_history(mac, 'STATIC_ASSIGNED', old_ip, new_ip, hostname or None, client_id,
                                                 f"Назначен статический IP: {new_ip}", 
                                                 datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                            logging.info(f"Миграция IP для MAC {mac}, client_id {client_id or 'не указан'}: {old_ip} -> {new_ip}")
                            continue
                    new_ip = self._get_new_dynamic_ip(cursor, pool_start_int, pool_end_int)
                    if new_ip:
                        cursor.execute("UPDATE leases SET ip = ?, lease_type = 'DYNAMIC' WHERE mac = ?", (new_ip, mac))
                        self._insert_history(mac, 'DYNAMIC_ASSIGNED', old_ip, new_ip, hostname or None, client_id,
                                             f"Выдана новая аренда: IP {new_ip} до {(datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}",
                                             datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                        logging.info(f"Назначен новый IP для MAC {mac}, client_id {client_id or 'не указан'}: {old_ip} -> {new_ip} (ДИНАМИЧЕСКИЙ)")
                    else:
                        logging.error(f"Нет свободных IP для миграции MAC {mac}, client_id {client_id or 'не указан'}")
                except Exception as e:
                    logging.error(f"Ошибка при миграции IP для MAC {mac}, client_id {client_id or 'не указан'}: {e}")
            conn.commit()

    def _insert_history(self, mac, action, ip=None, new_ip=None, name=None, client_id=None, description=None, timestamp=None, new_name=None, change_channel='DHCP'):
        with self.get_history_connection() as conn:
            cursor = conn.cursor()
            if timestamp is None:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute('''
                INSERT INTO lease_history (mac, action, ip, new_ip, name, client_id, description, timestamp, new_name, change_channel)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (mac, action, ip, new_ip, name, client_id, description, timestamp, new_name, change_channel))
            conn.commit()

    def mark_expired_leases(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT mac, ip, hostname, client_id FROM leases WHERE lease_type = 'DYNAMIC' AND expire_at <= ? AND is_expired = 0 AND deleted_at IS NULL", (current_time,))
            expired_leases = cursor.fetchall()
            for mac, ip, hostname, client_id in expired_leases:
                self._insert_history(mac, 'LEASE_EXPIRED', ip, None, hostname or None, client_id,
                                     f"Аренда IP {ip} истекла",
                                     current_time)
            cursor.execute("""
                UPDATE leases
                SET is_expired = 1,
                    ip = NULL,
                    updated_at = ?
                WHERE lease_type = 'DYNAMIC' AND expire_at <= ? AND is_expired = 0 AND deleted_at IS NULL
            """, (current_time, current_time))
            conn.commit()
        logging.debug("Помечены истёкшие динамические аренды.")

        # Вызываем очистку устаревших записей истории
        self.clean_old_history()

    def mark_lease_expired(self, mac, ip, client_id=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT hostname, lease_type, client_id FROM leases WHERE mac = ? AND ip = ? AND deleted_at IS NULL", (mac, ip))
            row = cursor.fetchone()
            if row:
                hostname, lease_type, db_client_id = row
                if lease_type == 'DYNAMIC':
                    self._insert_history(mac, 'LEASE_RELEASED', ip, None, hostname or None, client_id,
                                         f"Аренда IP {ip} освобождена клиентом",
                                         current_time)
                    cursor.execute("""
                        UPDATE leases
                        SET is_expired = 1,
                            ip = NULL,
                            updated_at = ?
                        WHERE mac = ? AND ip = ?
                    """, (current_time, mac, ip))
                    conn.commit()
                    logging.info(f"Аренда помечена как истёкшая для MAC {mac}, client_id {client_id or 'не указан'}, IP {ip} (ОСВОБОЖДЕНА)")
                else:
                    logging.warning(f"RELEASE проигнорирован для статической аренды MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")
            else:
                logging.warning(f"RELEASE проигнорирован, клиент не найден с MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")

    def is_device_blocked(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT is_blocked FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            return row and row[0] == 1

    def block_device(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT hostname, ip, client_id FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                hostname, ip, client_id = row
                cursor.execute("""
                    UPDATE leases
                    SET is_blocked = 1,
                        ip = NULL,
                        is_expired = 1,
                        updated_at = ?
                    WHERE mac = ?
                """, (current_time, mac))
                self._insert_history(mac, 'DEVICE_BLOCKED', ip, None, hostname or None, client_id,
                                     f"Устройство заблокировано, IP {ip or 'не назначен'} освобождён",
                                     current_time)
                conn.commit()
                logging.info(f"Устройство заблокировано для MAC {mac}, client_id {client_id or 'не указан'}")
            else:
                logging.warning(f"Невозможно заблокировать устройство, MAC {mac} не найден")

    def unblock_device(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT hostname, client_id FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                hostname, client_id = row
                cursor.execute("""
                    UPDATE leases
                    SET is_blocked = 0,
                        updated_at = ?
                    WHERE mac = ?
                """, (current_time, mac))
                self._insert_history(mac, 'DEVICE_UNBLOCKED', None, None, hostname or None, client_id,
                                     f"Устройство разблокировано",
                                     current_time)
                conn.commit()
                logging.info(f"Устройство разблокировано для MAC {mac}, client_id {client_id or 'не указан'}")
            else:
                logging.warning(f"Невозможно разблокировать устройство, MAC {mac} не найден")

    def is_device_trusted(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT trust_flag FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            return row and row[0] == 1

    def set_trust_flag(self, mac, trust_flag, change_channel='WEB'):
        if trust_flag not in [0, 1]:
            logging.error(f"Неверное значение trust_flag для MAC {mac}: {trust_flag}. Должно быть 0 или 1.")
            return False
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            cursor.execute("""
                SELECT hostname, ip, client_id, trust_flag 
                FROM leases 
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            
            if not row:
                logging.warning(f"Устройство с MAC {mac} не найдено, невозможно изменить trust_flag")
                return False
            
            hostname, ip, client_id, old_trust_flag = row
            
            if old_trust_flag == trust_flag:
                logging.info(f"Статус доверенности для MAC {mac} уже равен {trust_flag}, изменение не требуется")
                return True
            
            # Обновляем флаг
            cursor.execute("""
                UPDATE leases 
                SET trust_flag = ?,
                    updated_at = ?
                WHERE mac = ?
            """, (trust_flag, current_time, mac))

            if trust_flag == 1:
                description = f"Устройство {hostname or 'неизвестное'} признано доверенным."
                action = 'TRUST_CHANGED'
            else:
                description = f"Устройство {hostname or 'неизвестное'} перестало быть доверенным."
                action = 'TRUST_CHANGED'

            self._insert_history(
                mac=mac,
                action=action,
                ip=ip,
                new_ip=None,
                name=hostname or None,
                client_id=client_id,
                description=description,
                timestamp=current_time,
                change_channel=change_channel
            )
            
            conn.commit()
            logging.info(f"Статус доверенности изменён для MAC {mac}, client_id {client_id or 'не указан'}: {old_trust_flag} -> {trust_flag}, change_channel {change_channel}")
            return True

    def find_free_ip(self, mac, client_id, pool_start_int, pool_end_int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self.mark_expired_leases()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            cursor.execute("""
                SELECT ip, expire_at, is_expired, lease_type, deleted_at, hostname, client_id
                FROM leases
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            if row:
                ip, expire_at, is_expired, lease_type, deleted_at, hostname, db_client_id = row
                if lease_type == 'STATIC' and ip:
                    logging.debug(f"Статическая аренда для MAC {mac}, client_id {client_id or 'не указан'}: IP {ip} (разрешён вне пула)")
                    return ip, lease_type
                elif lease_type == 'DYNAMIC' and not is_expired and expire_at and expire_at > current_time:
                    ip_int = self.ip_to_int(ip)
                    if pool_start_int <= ip_int <= pool_end_int:
                        logging.debug(f"Существующая динамическая аренда для MAC {mac}, client_id {client_id or 'не указан'}: IP {ip}")
                        return ip, lease_type
                    else:
                        logging.warning(f"Существующий IP {ip} для MAC {mac} вне пула ({self.int_to_ip(pool_start_int)}-{self.int_to_ip(pool_end_int)}). Назначается новый IP.")
            return self._get_new_dynamic_ip(cursor, pool_start_int, pool_end_int), 'DYNAMIC'

    def _get_new_dynamic_ip(self, cursor, pool_start_int, pool_end_int):
        cursor.execute("SELECT ip FROM leases WHERE ip IS NOT NULL AND deleted_at IS NULL")
        used_ips = {self.ip_to_int(row[0]) for row in cursor.fetchall() if row[0]}
        for ip_int in range(pool_start_int, pool_end_int + 1):
            if ip_int not in used_ips:
                ip = self.int_to_ip(ip_int)
                logging.debug(f"Найден свободный IP: {ip}")
                return ip
        logging.error(f"Нет свободных IP в пуле {self.int_to_ip(pool_start_int)}-{self.int_to_ip(pool_end_int)}")
        return None

    @staticmethod
    def ip_to_int(ip):
        if not ip or ip == 'None':
            logging.error(f"Недопустимый IP-адрес: {ip}")
            raise ValueError("IP address cannot be None or empty")
        try:
            return struct.unpack("!I", socket.inet_aton(ip))[0]
        except socket.error as e:
            logging.error(f"Ошибка при преобразовании IP {ip} в целое число: {e}")
            raise ValueError(f"Invalid IP address: {ip}")

    @staticmethod
    def int_to_ip(int_ip):
        return socket.inet_ntoa(struct.pack("!I", int_ip))

    def get_last_activity_time(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT expire_at, updated_at
                FROM leases
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            if row:
                expire_at, updated_at = row
                if expire_at:
                    try:
                        return datetime.strptime(expire_at, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        pass
                if updated_at:
                    try:
                        return datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        pass
            return None

    def get_hostname(self, mac):
        """Получение имени хоста из базы данных по MAC-адресу."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else None

    def create_lease(self, mac, ip, hostname=None, lease_type='DYNAMIC', client_id=None, create_channel='DHCP_REQUEST', change_channel='DHCP'):
        if self.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, создание аренды невозможно.")
            return
        if ip is None:
            logging.error(f"Невозможно создать новую аренду для MAC {mac} без IP-адреса")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            expire_at = None if lease_type == 'STATIC' else (datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            is_expired = 0
            is_custom_hostname = 1 if hostname and change_channel == 'WEB' else 0
            is_trusted_device = 1 if change_channel == 'WEB' and lease_type == 'STATIC' else 0
            cursor.execute("""
                INSERT INTO leases (mac, ip, hostname, client_id, created_at, updated_at, expire_at, is_expired, lease_type, deleted_at, create_channel, is_custom_hostname, trust_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (mac, ip, hostname, client_id, current_time_str, current_time_str, expire_at, is_expired, lease_type, None, create_channel, is_custom_hostname, is_trusted_device))
            
            description = f"Создан новый клиент, имя хоста: {hostname or 'не указано'}"
            if create_channel == 'STATIC_LEASE':
                description = f"Создан новый клиент через статическую привязку, IP {ip}, имя хоста: {hostname or 'не указано'}"
            self._insert_history(mac, 'CLIENT_CREATE', None, ip, hostname or None, client_id, description, current_time_str, change_channel=change_channel)
            
            if lease_type == 'DYNAMIC':
                lease_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                self._insert_history(mac, 'LEASE_ISSUED', None, ip, hostname or None, client_id,
                                    f"Выдана новая аренда: IP {ip} до {expire_at}",
                                    lease_time_str, change_channel=change_channel)
            
            if create_channel != 'STATIC_LEASE':
                self.telegram_notifier.notify(mac, ip, hostname, True)
            conn.commit()
            logging.info(f"Создана новая аренда для MAC {mac}, client_id {client_id or 'не указан'}: IP {ip}, тип {lease_type}, create_channel {create_channel}, change_channel {change_channel}")
        
    def update_ip(self, mac, new_ip, client_id=None, change_channel='DHCP'):
        if self.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, обновление IP невозможно.")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("""
                SELECT ip, hostname, lease_type, client_id, is_expired, expire_at
                FROM leases
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            if not row:
                logging.error(f"Аренда для MAC {mac} не найдена")
                return
            old_ip, hostname, lease_type, old_client_id, is_expired, old_expire_at = row
            if new_ip == old_ip:
                return
            if new_ip is None:
                logging.error(f"Невозможно установить IP в NULL для MAC {mac}")
                return
            
            new_expire_at = None
            new_is_expired = 0
            if lease_type == 'DYNAMIC':
                new_expire_at = (datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                # if is_expired == 1:
                #     self._insert_history(mac, 'LEASE_RENEWED', old_ip, new_ip, hostname or None, client_id or old_client_id,
                #                         f"Аренда IP {new_ip} возобновлена до {new_expire_at}",
                #                         current_time_str, change_channel=change_channel)
                #     logging.info(f"Аренда возобновлена для MAC {mac}, IP {new_ip}")
            
            cursor.execute("""
                UPDATE leases
                SET ip = ?,
                    expire_at = ?,
                    is_expired = ?,
                    updated_at = ?,
                    client_id = ?
                WHERE mac = ?
            """, (new_ip, new_expire_at, new_is_expired, current_time_str, client_id or old_client_id, mac))
            
            history_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if lease_type == 'STATIC':
                description = f"Назначен статический IP: {new_ip}"
                self._insert_history(mac, 'STATIC_ASSIGNED', old_ip, new_ip, hostname or None, client_id or old_client_id,
                                    description, history_time_str, change_channel=change_channel)
            else:
                description = f"Выдана новая аренда: IP {new_ip}"
                self._insert_history(mac, 'LEASE_ISSUED', old_ip, new_ip, hostname or None, client_id or old_client_id,
                                    description, history_time_str, change_channel=change_channel)
            conn.commit()
            logging.info(f"Обновлён IP для MAC {mac}, client_id {client_id or old_client_id or 'не указан'}: {old_ip} -> {new_ip}, is_expired сброшен на {new_is_expired}, change_channel {change_channel}")
    
    def update_hostname(self, mac, hostname, client_id=None, change_channel='DHCP'):
        if self.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, обновление имени хоста невозможно.")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("""
                SELECT hostname, ip, client_id, is_custom_hostname
                FROM leases
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            if not row:
                logging.error(f"Аренда для MAC {mac} не найдена")
                return
            old_hostname, ip, old_client_id, is_custom_hostname = row
            if hostname == old_hostname:
                return
            if change_channel == 'DHCP' and is_custom_hostname:
                return
            cursor.execute("""
                UPDATE leases
                SET hostname = ?,
                    updated_at = ?,
                    client_id = ?,
                    is_custom_hostname = ?
                WHERE mac = ?
            """, (hostname, current_time_str, client_id or old_client_id, 1 if change_channel == 'WEB' else is_custom_hostname, mac))
            self._insert_history(mac, 'HOSTNAME_UPDATED', ip, None, old_hostname or None, client_id or old_client_id,
                                f"Имя хоста изменено на {hostname}",
                                current_time_str, new_name=hostname, change_channel=change_channel)
            conn.commit()
            logging.info(f"Обновлено имя хоста для MAC {mac}, client_id {client_id or old_client_id or 'не указан'}: {old_hostname or 'не указано'} -> {hostname}, change_channel {change_channel}")
    
    def renew_lease(self, mac, client_id=None, change_channel='DHCP'):
        if self.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, продление аренды невозможно.")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("""
                SELECT ip, hostname, lease_type, client_id, expire_at
                FROM leases
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            if not row:
                logging.error(f"Аренда для MAC {mac} не найдена")
                return
            ip, hostname, lease_type, old_client_id, old_expire_at = row
            if lease_type != 'DYNAMIC':
                logging.debug(f"Продление проигнорировано для статической аренды MAC {mac}")
                return
            new_expire_at = (datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("""
                UPDATE leases
                SET expire_at = ?,
                    updated_at = ?,
                    client_id = ?
                WHERE mac = ?
            """, (new_expire_at, current_time_str, client_id or old_client_id, mac))
            self._insert_history(mac, 'LEASE_RENEWED', ip, None, hostname or None, client_id or old_client_id,
                                f"Аренда IP {ip} продлена до {new_expire_at}",
                                current_time_str, change_channel=change_channel)
            
            # Проверка time_diff для уведомления
            time_diff = self._get_time_diff(mac)
            if time_diff is not None:
                self.telegram_notifier.notify(mac, ip, hostname, False, time_diff)
            
            conn.commit()
            logging.info(f"Аренда продлена для MAC {mac}, client_id {client_id or old_client_id or 'не указан'}: IP {ip} до {new_expire_at}, change_channel {change_channel}")

    def update_lease_type(self, mac, lease_type, client_id=None, change_channel='DHCP'):
        if self.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, обновление типа аренды невозможно.")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("""
                SELECT ip, hostname, lease_type, client_id
                FROM leases
                WHERE mac = ? AND deleted_at IS NULL
            """, (mac,))
            row = cursor.fetchone()
            if not row:
                logging.error(f"Аренда для MAC {mac} не найдена")
                return
            ip, hostname, old_lease_type, old_client_id = row
            if lease_type == old_lease_type:
                return
            expire_at = None if lease_type == 'STATIC' else (datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            is_expired = 0
            cursor.execute("""
                UPDATE leases
                SET lease_type = ?,
                    expire_at = ?,
                    is_expired = ?,
                    updated_at = ?,
                    client_id = ?
                WHERE mac = ?
            """, (lease_type, expire_at, is_expired, current_time_str, client_id or old_client_id, mac))
            action = 'STATIC_ASSIGNED' if lease_type == 'STATIC' else 'DYNAMIC_ASSIGNED'
            description = f"Назначен статический IP: {ip}" if lease_type == 'STATIC' else f"Выдана новая аренда: IP {ip} до {expire_at}"
            self._insert_history(mac, action, ip, ip, hostname or None, client_id or old_client_id,
                                description, current_time_str, change_channel=change_channel)
            if lease_type == 'DYNAMIC':
                time_diff = self._get_time_diff(mac)
                if time_diff is not None:
                    self.telegram_notifier.notify(mac, ip, hostname, False, time_diff)
            conn.commit()
            logging.info(f"Обновлён тип аренды для MAC {mac}, client_id {client_id or old_client_id or 'не указан'}: {old_lease_type} -> {lease_type}, change_channel {change_channel}")
    
    def decline_lease(self, mac, ip, client_id=None, pool_start_int=None, pool_end_int=None):
        if self.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, обработка DECLINE невозможна.")
            return None
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT hostname, lease_type, client_id FROM leases WHERE mac = ? AND ip = ? AND deleted_at IS NULL", (mac, ip))
            row = cursor.fetchone()
            if row:
                hostname, lease_type, db_client_id = row
                if lease_type == 'DYNAMIC':
                    self._insert_history(mac, 'DECLINE', ip, None, hostname or None, client_id or db_client_id,
                                        f"Клиент отклонил предложенный IP {ip}", current_time, change_channel='DHCP')
                    cursor.execute("""
                        UPDATE leases
                        SET is_expired = 1,
                            ip = NULL,
                            updated_at = ?
                        WHERE mac = ? AND ip = ?
                    """, (current_time, mac, ip))
                    logging.info(f"DHCPDECLINE обработан для MAC {mac}, client_id {client_id or 'не указан'}: IP {ip} освобождён")
                else:
                    logging.warning(f"DECLINE проигнорирован для статической аренды MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")
                    return None
            else:
                logging.warning(f"DECLINE проигнорирован, клиент не найден с MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")
                return None
            
            # Предложить новый IP, если указаны границы пула
            new_ip = None
            if pool_start_int and pool_end_int:
                new_ip = self._get_new_dynamic_ip(cursor, pool_start_int, pool_end_int)
                if new_ip:
                    cursor.execute("""
                        UPDATE leases
                        SET ip = ?,
                            is_expired = 0,
                            expire_at = ?,
                            updated_at = ?
                        WHERE mac = ?
                    """, (new_ip, (datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], current_time, mac))
                    self._insert_history(mac, 'LEASE_ISSUED', None, new_ip, hostname or None, client_id or db_client_id,
                                        f"Выдана новая аренда: IP {new_ip} до {(datetime.now() + timedelta(seconds=self.config['lease_time'])).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}",
                                        current_time, change_channel='DHCP')
                    logging.info(f"Назначен новый IP для MAC {mac}, client_id {client_id or 'не указан'}: {new_ip} после DECLINE")
                else:
                    logging.error(f"Нет свободных IP для MAC {mac}, client_id {client_id or 'не указан'} после DECLINE")
            conn.commit()
            return new_ip

    def nak_lease(self, mac, ip, client_id=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT hostname, lease_type, client_id FROM leases WHERE mac = ? AND ip = ? AND deleted_at IS NULL", (mac, ip))
            row = cursor.fetchone()
            if row:
                hostname, lease_type, db_client_id = row
                self._insert_history(mac, 'NAK', ip, None, hostname or None, client_id or db_client_id,
                                    f"Отказ на выдачу запрошенного IP {ip}", current_time, change_channel='DHCP')
                logging.info(f"DHCPNAK обработан для MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")
            conn.commit()

    def inform_lease(self, mac, ip, client_id=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT hostname, lease_type, client_id FROM leases WHERE mac = ? AND ip = ? AND deleted_at IS NULL", (mac, ip))
            row = cursor.fetchone()
            if row:
                hostname, lease_type, db_client_id = row
                self._insert_history(mac, 'INFORM', ip, None, hostname or None, client_id or db_client_id,
                                    f"Предоставлены сетевые параметры для клиента с IP {ip}", current_time, change_channel='DHCP')
                logging.info(f"DHCPINFORM обработан для MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")
            conn.commit()

    def _get_time_diff(self, mac):
        last_activity_time = self.get_last_activity_time(mac)
        if last_activity_time:
            time_diff = datetime.now() - last_activity_time
            if time_diff < self.telegram_notifier.inactive_period:
                return None
            return time_diff
        return None

    def get_lease_type(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT lease_type FROM leases WHERE mac = ?", (mac,))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_all_leases(self, include_deleted=False, not_expired=True):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self.mark_expired_leases()
            query = """
                SELECT id, client_id, mac, hostname, ip, lease_type, expire_at, is_expired, 
                    updated_at, created_at, create_channel, deleted_at, is_blocked, is_custom_hostname, trust_flag
                FROM leases
            """
            conditions = []
            if not include_deleted:
                conditions.append("deleted_at IS NULL")
            if not_expired:
                conditions.append("is_expired != 1")
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            return columns, cursor.fetchall()

    def delete(self, mac):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            cursor.execute("SELECT ip, hostname, client_id FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                ip, hostname, client_id = row
                cursor.execute("""
                    UPDATE leases
                    SET deleted_at = ?,
                        lease_type = 'DYNAMIC'
                    WHERE mac = ?
                """, (current_time, mac))
                self._insert_history(mac, 'DEVICE_DELETED', ip, None, hostname or None, client_id,
                                     f"Устройство удалено",
                                     current_time)
                conn.commit()
                logging.info(f"Удалено устройство MAC {mac}, client_id {client_id or 'не указан'}")

    def get_lease_history(self, mac, limit=10):
        with self.get_history_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, action, ip, new_ip, name, new_name, description, timestamp 
                FROM lease_history 
                WHERE mac = ? 
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (mac, limit))
            return cursor.fetchall()
    
    def get_client_by_ip(self, ip):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT mac, ip, hostname, client_id, created_at, updated_at, expire_at, is_expired, lease_type, is_blocked, is_custom_hostname, trust_flag
                FROM leases
                WHERE ip = ? AND deleted_at IS NULL
            """, (ip,))
            row = cursor.fetchone()
            if row:
                return {
                    'mac': row[0],
                    'ip': row[1],
                    'hostname': row[2] or None,
                    'client_id': row[3],
                    'created_at': row[4],
                    'updated_at': row[5],
                    'expire_at': row[6],
                    'is_expired': row[7],
                    'lease_type': row[8],
                    'is_blocked': row[9],
                    'is_custom_hostname': row[10],
                    'trust_flag': row[11]
                }
            return None

class AuthManager:
    def __init__(self, auth_db_file):
        self.auth_db_file = auth_db_file
        self.init_auth_db()

    def get_auth_connection(self):
        conn = sqlite3.connect(self.auth_db_file, check_same_thread=False)
        return conn

    def init_auth_db(self):
        with self.get_auth_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL
                )
            ''')
        logging.info("Инициализирована база данных аутентификации SQLite.")

    def user_exists(self):
        with self.get_auth_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0] > 0

    def create_user(self, username, password_hash):
        with self.get_auth_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
            conn.commit()

    def get_user(self, username):
        with self.get_auth_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
            row = cursor.fetchone()
            return row[0] if row else None
        