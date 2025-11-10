import socket
import struct
import binascii
import logging
import threading
import netifaces
import time
from collections import defaultdict
from datetime import datetime, timedelta
from influxdb import InfluxDBService

class DHCPServer:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.stop_event = threading.Event()
        self.thread = None
        self.lease_check_thread = None
        self.metrics_thread = None  # Поток для отправки метрик
        self.pool_start_int = self.ip_to_int(config['pool_start'])
        self.pool_end_int = self.ip_to_int(config['pool_end'])
        self.interface = config.get('interface', None)
        self.discover_cache = {}  # Кэш для DHCPDISCOVER: { (xid, mac): { 'packet': bytes, 'expire_at': datetime } }
        self.request_cache = {}  # Кэш для DHCPREQUEST: { (xid, mac, requested_ip): { 'packet': bytes, 'expire_at': datetime } }
        self.inform_cache = {}  # Кэш для DHCPINFORM: { (xid, mac, ciaddr): { 'packet': bytes, 'expire_at': datetime } }
        self.cache_ttl = config.get('cache_ttl', 30)  # TTL кэша в секундах
        self.metrics = defaultdict(int)  # Счётчик для каждого типа сообщения
        self.influxdb = InfluxDBService(config)  # Инициализация InfluxDB
        logging.info("DHCP-сервер инициализирован с конфигурацией: %s", config)

    @staticmethod
    def ip_to_int(ip):
        if ip is None:
            return None
        try:
            return struct.unpack("!I", socket.inet_aton(ip))[0]
        except socket.error:
            logging.error(f"Неверный формат IP-адреса: {ip}")
            return None

    @staticmethod
    def int_to_ip(int_ip):
        return socket.inet_ntoa(struct.pack("!I", int_ip))

    def find_free_ip(self, mac, client_id=None):
        if self.db_manager.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, в выдаче IP отказано.")
            return None
        ip, lease_type = self.db_manager.find_free_ip(mac, client_id, self.pool_start_int, self.pool_end_int)
        if ip:
            ip_int = self.ip_to_int(ip)
            if not (self.pool_start_int <= ip_int <= self.pool_end_int) and lease_type == 'DYNAMIC':
                logging.warning(f"IP {ip} вне пула ({self.int_to_ip(self.pool_start_int)}-{self.int_to_ip(self.pool_end_int)}). Ищем другой IP.")
                ip = None
        logging.debug(f"Поиск свободного IP для MAC {mac}, client_id {client_id or 'не указан'}: возвращено {ip}")
        return ip

    def update_lease(self, mac, ip, hostname=None, lease_type=None, client_id=None):
        if self.db_manager.is_device_blocked(mac):
            logging.warning(f"Устройство с MAC {mac}, client_id {client_id or 'не указан'} заблокировано, обновление аренды невозможно.")
            return
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ip, lease_type, hostname, client_id, deleted_at
                FROM leases
                WHERE mac = ?
            """, (mac,))
            row = cursor.fetchone()
            
            if row:
                old_ip, old_lease_type, old_hostname, old_client_id, deleted_at = row
                if deleted_at is not None:
                    logging.info(f"Восстановление удаленной записи для MAC {mac}")
                    cursor.execute("""
                        UPDATE leases 
                        SET deleted_at = NULL
                        WHERE mac = ?
                    """, (mac,))
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    self.db_manager._insert_history(
                        mac=mac,
                        action='DEVICE_RESTORED',
                        ip=None,
                        new_ip=None,
                        name=old_hostname or hostname,
                        new_name=None,
                        description=f"Устройство восстановлено после удаления",
                        timestamp=current_time,
                        change_channel='DHCP'
                    )
                    conn.commit()
                    self.db_manager.update_ip(mac, ip, client_id, change_channel='DHCP')
                    return
                else:
                    if ip == old_ip and lease_type == old_lease_type:
                        self.db_manager.renew_lease(mac, client_id, change_channel='DHCP')
                        if hostname and hostname != old_hostname:
                            self.db_manager.update_hostname(mac, hostname, client_id, change_channel='DHCP')
                        if client_id and client_id != old_client_id:
                            cursor.execute("UPDATE leases SET client_id = ? WHERE mac = ?", (client_id, mac))
                    else:
                        if ip != old_ip and ip is not None:
                            self.db_manager.update_ip(mac, ip, client_id, change_channel='DHCP')
                        if hostname and hostname != old_hostname:
                            self.db_manager.update_hostname(mac, hostname, client_id, change_channel='DHCP')
                        if lease_type and lease_type != old_lease_type:
                            self.db_manager.update_lease_type(mac, lease_type, client_id, change_channel='DHCP')
            else:
                self.db_manager.create_lease(mac, ip, hostname, lease_type or 'DYNAMIC', client_id, 
                                            create_channel='DHCP_REQUEST', change_channel='DHCP')
            
            conn.commit()
        
        logging.info(f"Обновлена аренда для MAC {mac}, client_id {client_id or 'не указан'}: IP {ip}, тип {lease_type or 'не изменён'}, hostname {hostname or 'не изменён'}")

    def release_lease(self, mac, ciaddr, client_id=None):
        if ciaddr == '0.0.0.0':
            logging.warning(f"Игнорируется RELEASE с недействительным IP {ciaddr} для MAC {mac}, client_id {client_id or 'не указан'}")
            return
        self.db_manager.mark_lease_expired(mac, ciaddr, client_id)
        logging.info(f"Обработан RELEASE для MAC {mac}, client_id {client_id or 'не указан'}: IP {ciaddr} освобождён.")
    
    def decline_lease(self, mac, ip, client_id=None):
        new_ip = self.db_manager.decline_lease(mac, ip, client_id, self.pool_start_int, self.pool_end_int)
        if new_ip:
            logging.info(f"Новый IP {new_ip} предложен для MAC {mac} после DECLINE")
            return new_ip
        return None

    def nak_lease(self, mac, ip, client_id=None):
        self.db_manager.nak_lease(mac, ip, client_id)
        logging.info(f"Отправлен DHCPNAK для MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")

    def inform_lease(self, mac, ip, client_id=None):
        self.db_manager.inform_lease(mac, ip, client_id)
        logging.info(f"DHCPINFORM обработан для MAC {mac}, client_id {client_id or 'не указан'}, IP {ip}")

    def inform_client(self, mac, ciaddr, xid, chaddr, hostname, client_id=None):
        self.inform_lease(mac, ciaddr, client_id)
        options = self.get_options(5, '0.0.0.0')
        packet = self.build_packet(2, 1, 6, xid, ciaddr, '0.0.0.0', self.config['server_ip'], chaddr, options)
        logging.debug(f"Сформирован DHCPACK для INFORM от MAC {mac}, client_id {client_id or 'не указан'}")
        return packet

    def periodic_lease_check(self):
        while not self.stop_event.is_set():
            try:
                self.db_manager.mark_expired_leases()
                logging.debug("Периодическая проверка истёкших сроков аренд выполнена.")
                time.sleep(self.config['expire_check_period'])
            except Exception as e:
                logging.error(f"Ошибка в периодической проверке срока аренд: {e}")
                time.sleep(60)

    def periodic_metrics_flush(self):
        """Фоновый поток для отправки метрик в InfluxDB"""
        while not self.stop_event.is_set():
            try:
                if self.metrics:
                    self.influxdb.write_dhcp_metrics(self.metrics)
                    self.metrics.clear()
                time.sleep(self.influxdb.metrics_interval)
            except Exception as e:
                logging.error(f"Ошибка в отправке метрик в InfluxDB: {e}")
                time.sleep(self.influxdb.metrics_interval)

    def build_packet(self, op, htype, hlen, xid, ciaddr, yiaddr, siaddr, chaddr, options):
        packet = struct.pack('!BBBBIHH4s4s4s4s', op, htype, hlen, 0, xid, 0, 0,
                             socket.inet_aton(ciaddr), socket.inet_aton(yiaddr),
                             socket.inet_aton(siaddr), socket.inet_aton('0.0.0.0'))
        packet += chaddr + b'\x00' * (16 - len(chaddr))
        packet += b'\x00' * 192
        packet += b'\x63\x82\x53\x63'
        packet += options
        logging.debug(f"Сформирован пакет (op={op}, yiaddr={yiaddr}, siaddr={siaddr}): {binascii.hexlify(packet)}")
        return packet

    def get_options(self, msg_type, yiaddr):
        options = b'\x35\x01' + struct.pack('B', msg_type)
        options += b'\x36\x04' + socket.inet_aton(self.config['server_ip'])
        options += b'\x01\x04' + socket.inet_aton(self.config['subnet_mask'])
        options += b'\x03\x04' + socket.inet_aton(self.config['gateway'])
        dns_opt = b'\x06' + struct.pack('B', len(self.config['dns_servers']) * 4)
        for dns in self.config['dns_servers']:
            dns_opt += socket.inet_aton(dns)
        options += dns_opt
        options += b'\x33\x04' + struct.pack('!I', self.config['lease_time'])
        options += b'\x3a\x04' + struct.pack('!I', self.config['lease_time'] // 2)
        options += b'\x3b\x04' + struct.pack('!I', self.config['lease_time'] * 7 // 8)
        domain_bytes = self.config['domain_name'].encode('ascii')
        options += b'\x0f' + struct.pack('B', len(domain_bytes)) + domain_bytes
        options += b'\xFF'
        logging.debug(f"Сформированы опции для типа {msg_type} (yiaddr={yiaddr}): {binascii.hexlify(options)}")
        return options

    def parse_packet(self, data):
        op, htype, hlen, hops, xid, secs, flags, ciaddr, yiaddr, siaddr, giaddr = struct.unpack('!BBBBIHH4s4s4s4s', data[:28])
        ciaddr = socket.inet_ntoa(ciaddr)
        chaddr = data[28:28 + hlen]
        options_start = 28 + 16 + 64 + 128 + 4
        options = data[options_start:]
        msg_type = None
        requested_ip = None
        hostname = None
        client_id = None
        i = 0
        while i < len(options) and options[i] != 0xff:
            code = options[i]
            length = options[i + 1]
            value = options[i + 2 : i + 2 + length]
            if code == 53:
                msg_type = value[0]
            elif code == 50:
                requested_ip = socket.inet_ntoa(value)
            elif code == 12:
                hostname = value.decode('ascii', errors='ignore')
            elif code == 61:
                client_id = binascii.hexlify(value).decode('utf-8')
            i += 2 + length
        mac = ':'.join(binascii.hexlify(chaddr).decode('utf-8')[i:i+2] for i in range(0, 12, 2))
        logging.debug(f"Разобран пакет: msg_type={msg_type}, xid={xid:08x}, chaddr={mac}, client_id={client_id or 'не указан'}, requested_ip={requested_ip}, hostname={hostname}")
        return msg_type, xid, chaddr, requested_ip, hostname, ciaddr, client_id

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(1)
        try:
            sock.bind(('0.0.0.0', 67))
            logging.info("DHCP-сервер запущен и слушает на порту 67...")
        except Exception as e:
            logging.error(f"Не удалось привязаться к порту 67: {e}")
            return

        if self.interface:
            try:
                if self.interface in netifaces.interfaces():
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, self.interface.encode() + b'\x00')
                    logging.info(f"DHCP-сервер успешно привязан к интерфейсу {self.interface}")
                else:
                    logging.warning(f"Интерфейс {self.interface} не найден. Доступные интерфейсы: {netifaces.interfaces()}. Продолжаем работу без привязки к интерфейсу.")
            except Exception as e:
                logging.warning(f"Ошибка при привязке к интерфейсу {self.interface}: {e}. Продолжаем работу без привязки к интерфейсу.")
        else:
            logging.info("Интерфейс не указан в конфигурации, работаем без привязки к конкретному интерфейсу.")

        while not self.stop_event.is_set():
            try:
                data, addr = sock.recvfrom(1024)
                if len(data) < 240:
                    logging.warning(f"Получен неверный пакет (слишком короткий) от {addr}")
                    continue

                msg_type, xid, chaddr, requested_ip, hostname, ciaddr, client_id = self.parse_packet(data)
                mac = ':'.join(binascii.hexlify(chaddr).decode('utf-8')[i:i+2] for i in range(0, 12, 2))
                self.metrics[msg_type] += 1

                logging.info(
                    f"\n<< Получен запрос {msg_type_to_str(msg_type)}\n"
                    f"<< {explain_dhcp_type(msg_type)}\n"
                    f"<< MAC: {mac}, client_id: {client_id or 'не указан'}, xid: {xid or 'не указан'}, запрошенный IP: {requested_ip or 'не указан'}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                )

                if self.db_manager.is_device_blocked(mac):
                    self.metrics[6] += 1
                    self.nak_lease(mac, requested_ip or ciaddr, client_id)
                    options = b'\x35\x01\x06'
                    packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', '0.0.0.0', self.config['server_ip'], chaddr, options)
                    sock.sendto(packet, ('255.255.255.255', 68))
                    logging.info(
                        f"\n>> Отправлен ответ NAK\n"
                        f">> Сервер отказывает в выдаче IP-адреса\n"
                        f">> MAC: {mac}, client_id: {client_id or 'не указан'}, причина: устройство заблокировано"
                    )
                    continue
                
                current_time = datetime.now()

                if msg_type == 1:  # DISCOVER
                    cache_key = (xid, mac)
                    if cache_key in self.discover_cache and self.discover_cache[cache_key]['expire_at'] > current_time:
                        self.metrics[2] += 1
                        packet = self.discover_cache[cache_key]['packet']
                        sock.sendto(packet, ('255.255.255.255', 68))
                        logging.info(
                            f"\n>> Отправлен ответ OFFER [Кэш]\n"
                            f">> Сервер предлагает IP-адрес устройству\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, предложенный IP: {requested_ip or 'не указан'}, тип аренды: DYNAMIC (временная аренда IP из пула, истекает через указанное время), имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                        )
                        continue
                    
                    yiaddr = self.find_free_ip(mac, client_id)
                    if yiaddr:
                        ip_int = self.ip_to_int(yiaddr)
                        lease_type = self.db_manager.get_lease_type(mac) or 'DYNAMIC'
                        if not (self.pool_start_int <= ip_int <= self.pool_end_int) and lease_type == 'DYNAMIC':
                            logging.error(
                                f"\n>> Ответ не отправлен\n"
                                f">> Предложенный IP вне пула\n"
                                f">> MAC: {mac}, client_id: {client_id or 'не указан'}, предложенный IP: {yiaddr}, пул: {self.int_to_ip(self.pool_start_int)}-{self.int_to_ip(self.pool_end_int)}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                            )
                            continue
                        
                        self.metrics[2] += 1
                        options = self.get_options(2, yiaddr)
                        packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', yiaddr, self.config['server_ip'], chaddr, options)
                        self.discover_cache[cache_key] = {
                            'packet': packet,
                            'expire_at': current_time + timedelta(seconds=self.cache_ttl)
                        }
                        sock.sendto(packet, ('255.255.255.255', 68))
                        logging.info(
                            f"\n>> Отправлен ответ OFFER\n"
                            f">> Сервер предлагает IP-адрес устройству\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, предложенный IP: {yiaddr}, тип аренды: {lease_type} ({explain_lease_type(lease_type)}), имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                        )
                    else:
                        logging.error(
                            f"\n>> Ответ не отправлен\n"
                            f">> Нет свободных IP-адресов\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                        )

                elif msg_type == 3:  # REQUEST
                    cache_key = (xid, mac, requested_ip)
                    if cache_key in self.request_cache and self.request_cache[cache_key]['expire_at'] > current_time:
                        self.metrics[5] += 1
                        packet = self.request_cache[cache_key]['packet']
                        sock.sendto(packet, ('255.255.255.255', 68))
                        logging.info(
                            f"\n>> Отправлен ответ ACK [Кэш]\n"
                            f">> Сервер подтверждает выдачу IP-адреса устройству\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, выданный IP: {requested_ip}, тип аренды: DYNAMIC (временная аренда IP из пула, истекает через указанное время), имя хоста: {hostname or 'не указано'}, время аренды: {self.config['lease_time']} секунд, адрес клиента: {addr}"
                        )
                        continue

                    with self.db_manager.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT ip, lease_type FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
                        row = cursor.fetchone()
                        if row and row[1] == 'STATIC':
                            static_ip = row[0]
                            if requested_ip and requested_ip != static_ip:
                                self.metrics[6] += 1
                                self.nak_lease(mac, requested_ip, client_id)
                                options = b'\x35\x01\x06'
                                packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', '0.0.0.0', self.config['server_ip'], chaddr, options)
                                sock.sendto(packet, ('255.255.255.255', 68))
                                logging.info(
                                    f"\n>> Отправлен ответ NAK\n"
                                    f">> Сервер отказывает в выдаче IP-адреса\n"
                                    f">> MAC: {mac}, client_id: {client_id or 'не указан'}, запрошенный IP: {requested_ip}, причина: статический IP {static_ip} назначен"
                                )
                                continue
                            yiaddr = static_ip
                            lease_type = 'STATIC'
                        else:
                            if requested_ip:
                                requested_ip_int = self.ip_to_int(requested_ip)
                                if requested_ip_int is None or not (self.pool_start_int <= requested_ip_int <= self.pool_end_int):
                                    self.metrics[6] += 1
                                    self.nak_lease(mac, requested_ip, client_id)
                                    options = b'\x35\x01\x06'
                                    packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', '0.0.0.0', self.config['server_ip'], chaddr, options)
                                    sock.sendto(packet, ('255.255.255.255', 68))
                                    logging.info(
                                        f"\n>> Отправлен ответ NAK\n"
                                        f">> Сервер отказывает в выдаче IP-адреса\n"
                                        f">> MAC: {mac}, client_id: {client_id or 'не указан'}, запрошенный IP: {requested_ip}, причина: IP вне пула ({self.int_to_ip(self.pool_start_int)}-{self.int_to_ip(self.pool_end_int)})"
                                    )
                                    continue
                                cursor.execute("SELECT mac FROM leases WHERE ip = ? AND mac != ? AND deleted_at IS NULL", (requested_ip, mac))
                                if cursor.fetchone():
                                    self.metrics[6] += 1
                                    self.nak_lease(mac, requested_ip, client_id)
                                    options = b'\x35\x01\x06'
                                    packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', '0.0.0.0', self.config['server_ip'], chaddr, options)
                                    sock.sendto(packet, ('255.255.255.255', 68))
                                    logging.info(
                                        f"\n>> Отправлен ответ NAK\n"
                                        f">> Сервер отказывает в выдаче IP-адреса\n"
                                        f">> MAC: {mac}, client_id: {client_id or 'не указан'}, запрошенный IP: {requested_ip}, причина: IP уже занят"
                                    )
                                    continue
                                yiaddr = requested_ip
                                lease_type = 'DYNAMIC'
                            else:
                                yiaddr = self.find_free_ip(mac, client_id)
                                lease_type = 'DYNAMIC'
                                if not yiaddr:
                                    self.metrics[6] += 1
                                    self.nak_lease(mac, requested_ip or ciaddr, client_id)
                                    options = b'\x35\x01\x06'
                                    packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', '0.0.0.0', self.config['server_ip'], chaddr, options)
                                    sock.sendto(packet, ('255.255.255.255', 68))
                                    logging.info(
                                        f"\n>> Отправлен ответ NAK\n"
                                        f">> Сервер отказывает в выдаче IP-адреса\n"
                                        f">> MAC: {mac}, client_id: {client_id or 'не указан'}, причина: нет свободных IP-адресов"
                                    )
                                    continue

                        self.update_lease(mac, yiaddr, hostname, lease_type, client_id)
                        self.metrics[5] += 1
                        options = self.get_options(5, yiaddr)
                        packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', yiaddr, self.config['server_ip'], chaddr, options)
                        self.request_cache[cache_key] = {
                            'packet': packet,
                            'expire_at': current_time + timedelta(seconds=self.cache_ttl)
                        }
                        sock.sendto(packet, ('255.255.255.255', 68))
                        logging.info(
                            f"\n>> Отправлен ответ ACK\n"
                            f">> Сервер подтверждает выдачу IP-адреса устройству\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, выданный IP: {yiaddr}, тип аренды: {lease_type} ({explain_lease_type(lease_type)}), имя хоста: {hostname or 'не указано'}, время аренды: {self.config['lease_time']} секунд, адрес клиента: {addr}"
                        )

                elif msg_type == 4:  # DECLINE
                    new_ip = self.decline_lease(mac, requested_ip, client_id)
                    if new_ip:
                        lease_type = 'DYNAMIC'
                        self.update_lease(mac, new_ip, hostname, lease_type, client_id)
                        self.metrics[5] += 1
                        options = self.get_options(5, new_ip)
                        packet = self.build_packet(2, 1, 6, xid, '0.0.0.0', new_ip, self.config['server_ip'], chaddr, options)
                        sock.sendto(packet, ('255.255.255.255', 68))
                        logging.info(
                            f"\n>> Отправлен ответ ACK\n"
                            f">> Сервер подтверждает выдачу IP-адреса устройству\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, выданный IP: {new_ip}, тип аренды: {lease_type} ({explain_lease_type(lease_type)}), имя хоста: {hostname or 'не указано'}, время аренды: {self.config['lease_time']} секунд, адрес клиента: {addr}"
                        )
                    else:
                        logging.warning(
                            f"\n>> Ответ не отправлен\n"
                            f">> Нет свободных IP-адресов для выдачи после DECLINE\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, отклонённый IP: {requested_ip}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                        )

                elif msg_type == 7:  # RELEASE
                    self.release_lease(mac, ciaddr, client_id)
                    logging.info(
                        f"\n>> Ответ не отправлен\n"
                        f">> Устройство освободило IP-адрес\n"
                        f">> MAC: {mac}, client_id: {client_id or 'не указан'}, освобождённый IP: {ciaddr}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr}"
                    )

                elif msg_type == 8:  # INFORM
                    cache_key = (xid, mac, ciaddr)
                    if cache_key in self.inform_cache and self.inform_cache[cache_key]['expire_at'] > current_time:
                        self.metrics[5] += 1
                        packet = self.inform_cache[cache_key]['packet']
                        sock.sendto(packet, (addr[0], 68))
                        logging.info(
                            f"\n>> Отправлен ответ ACK [Кэш]\n"
                            f">> Сервер подтверждает выдачу IP-адреса устройству\n"
                            f">> MAC: {mac}, client_id: {client_id or 'не указан'}, IP: {ciaddr}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr[0]}, отправлены сетевые параметры"
                        )
                        continue

                    packet = self.inform_client(mac, ciaddr, xid, chaddr, hostname, client_id)
                    self.inform_cache[cache_key] = {
                        'packet': packet,
                        'expire_at': current_time + timedelta(seconds=self.cache_ttl)
                    }
                    self.metrics[5] += 1
                    sock.sendto(packet, (addr[0], 68))
                    logging.info(
                        f"\n>> Отправлен ответ ACK\n"
                        f">> Сервер подтверждает выдачу IP-адреса устройству\n"
                        f">> MAC: {mac}, client_id: {client_id or 'не указан'}, IP: {ciaddr}, имя хоста: {hostname or 'не указано'}, адрес клиента: {addr[0]}, отправлены сетевые параметры"
                    )

            except socket.timeout:
                current_time = datetime.now()
                for cache in [self.discover_cache, self.request_cache, self.inform_cache]:
                    expired_keys = [k for k, v in cache.items() if v['expire_at'] <= current_time]
                    for key in expired_keys:
                        del cache[key]
                continue
            except Exception as e:
                logging.error(f"Ошибка в основном цикле DHCP-сервера: {e}")

        sock.close()
        logging.info("DHCP-сервер остановлен.")

    def start(self):
        if self.thread and self.thread.is_alive():
            logging.warning("Поток DHCP уже запущен.")
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self.run)
        self.lease_check_thread = threading.Thread(target=self.periodic_lease_check)
        self.metrics_thread = threading.Thread(target=self.periodic_metrics_flush)
        self.thread.start()
        self.lease_check_thread.start()
        self.metrics_thread.start()
        logging.info("Поток DHCP, поток проверки аренд и поток отправки метрик запущены.")

    def stop(self):
        if not self.thread or not self.thread.is_alive():
            logging.warning("Поток DHCP не запущен.")
        else:
            self.stop_event.set()
            self.thread.join()
            self.lease_check_thread.join()
            self.metrics_thread.join()
            self.influxdb.close()
            logging.info("Поток DHCP, поток проверки аренд и поток отправки метрик остановлены.")

def msg_type_to_str(msg_type):
    types = {1: 'DISCOVER', 2: 'OFFER', 3: 'REQUEST', 4: 'DECLINE', 5: 'ACK', 6: 'NAK', 7: 'RELEASE', 8: 'INFORM'}
    return types.get(msg_type, f'НЕИЗВЕСТНЫЙ({msg_type})')

def explain_dhcp_type(msg_type):
    explanations = {
        1: 'Устройство ищет DHCP-сервер и просит предложить IP-адрес',
        2: 'Сервер предлагает IP-адрес устройству',
        3: 'Устройство запрашивает подтверждение конкретного IP-адреса',
        4: 'Устройство отклоняет предложенный IP-адрес',
        5: 'Сервер подтверждает выдачу IP-адреса устройству',
        6: 'Сервер отказывает в выдаче IP-адреса',
        7: 'Устройство освобождает аренду IP-адреса',
        8: 'Устройство запрашивает только сетевые параметры без IP'
    }
    return explanations.get(msg_type, 'неизвестный тип запроса')

def explain_lease_type(lease_type):
    if lease_type == 'STATIC':
        return 'постоянная привязка IP к MAC-адресу, не истекает'
    elif lease_type == 'DYNAMIC':
        return 'временная аренда IP из пула, истекает через указанное время'
    else:
        return 'неизвестный тип аренды'