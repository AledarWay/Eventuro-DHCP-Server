import socket
import struct
import binascii
import time
import random
import argparse
import logging

class DHCPClient:
    def __init__(self, mac=None, hostname=None):
        # Настройка логирования
        logging.basicConfig(level=logging.DEBUG,
                           format='%(asctime)s - %(levelname)s - %(message)s',
                           handlers=[logging.StreamHandler()])
        
        # Генерация случайного MAC, если не указан
        if mac:
            self.mac = self.parse_mac(mac)
        else:
            self.mac = self.generate_random_mac()
        
        # Hostname или случайный, если не указан
        self.hostname = hostname or f"Debug-Client-{random.randint(1000, 9999)}"
        self.xid = random.randint(1, 0xFFFFFFFF)  # Случайный Transaction ID
        logging.info(f"Client initialized: MAC={self.mac_addr}, hostname={self.hostname}, xid={self.xid:08x}")

    def parse_mac(self, mac_str):
        """Преобразует MAC-адрес из строки (aa:bb:cc:dd:ee:ff) в байты."""
        mac_str = mac_str.replace(':', '')
        try:
            mac = binascii.unhexlify(mac_str)
            if len(mac) != 6:
                raise ValueError
            return mac
        except (ValueError, binascii.Error):
            logging.error("Invalid MAC address format. Use aa:bb:cc:dd:ee:ff")
            exit(1)

    def generate_random_mac(self):
        """Генерирует случайный MAC-адрес."""
        mac = [random.randint(0x00, 0xFF) for _ in range(6)]
        # Устанавливаем unicast бит (первый байт, младший бит = 0)
        mac[0] = mac[0] & 0xFE
        return bytes(mac)

    @property
    def mac_addr(self):
        """Возвращает MAC-адрес в читаемом виде."""
        return ':'.join(f'{b:02x}' for b in self.mac)

    def build_packet(self, msg_type, requested_ip=None):
        """Создаёт DHCP-пакет (DISCOVER или REQUEST)."""
        op = 1  # BOOTREQUEST
        htype = 1  # Ethernet
        hlen = 6  # MAC length
        hops = 0
        secs = 0
        flags = 0x8000  # Broadcast flag
        ciaddr = '0.0.0.0'
        yiaddr = '0.0.0.0'
        siaddr = '0.0.0.0'
        giaddr = '0.0.0.0'

        packet = struct.pack('!BBBBIHH4s4s4s4s', op, htype, hlen, hops, self.xid, secs, flags,
                             socket.inet_aton(ciaddr), socket.inet_aton(yiaddr),
                             socket.inet_aton(siaddr), socket.inet_aton(giaddr))
        packet += self.mac + b'\x00' * 10  # chaddr (16 bytes)
        packet += b'\x00' * 192  # sname + file
        packet += b'\x63\x82\x53\x63'  # DHCP magic cookie

        # Опции
        options = b'\x35\x01' + struct.pack('B', msg_type)  # DHCP Message Type
        options += b'\x0c' + struct.pack('B', len(self.hostname)) + self.hostname.encode('ascii')  # Host Name
        if msg_type == 3 and requested_ip:  # REQUEST
            options += b'\x32\x04' + socket.inet_aton(requested_ip)  # Requested IP
        options += b'\xff'  # End
        packet += options

        logging.debug(f"Built {['DISCOVER', 'OFFER', 'REQUEST', 'ACK'][msg_type-1]} packet: {binascii.hexlify(packet)}")
        return packet

    def parse_response(self, data):
        """Парсит ответ (OFFER или ACK) и возвращает детали аренды."""
        if len(data) < 240:
            logging.error("Received packet too short")
            return None

        _, _, _, _, xid, _, _, ciaddr, yiaddr, siaddr, _ = struct.unpack('!BBBBIHH4s4s4s4s', data[:28])
        if xid != self.xid:
            logging.warning("Received packet with wrong xid")
            return None

        options_start = 28 + 16 + 64 + 128 + 4
        options = data[options_start:]
        lease_info = {
            'ip': socket.inet_ntoa(yiaddr),
            'server_ip': socket.inet_ntoa(siaddr),
            'subnet_mask': None,
            'gateway': None,
            'dns_servers': [],
            'lease_time': None,
            'domain_name': None,
            'msg_type': None
        }

        i = 0
        while i < len(options) and options[i] != 0xff:
            code = options[i]
            length = options[i + 1]
            value = options[i + 2 : i + 2 + length]
            if code == 53:  # DHCP Message Type
                lease_info['msg_type'] = value[0]
            elif code == 1:  # Subnet Mask
                lease_info['subnet_mask'] = socket.inet_ntoa(value)
            elif code == 3:  # Router (Gateway)
                lease_info['gateway'] = socket.inet_ntoa(value)
            elif code == 6:  # DNS Servers
                for j in range(0, len(value), 4):
                    lease_info['dns_servers'].append(socket.inet_ntoa(value[j:j+4]))
            elif code == 51:  # Lease Time
                lease_info['lease_time'] = struct.unpack('!I', value)[0]
            elif code == 15:  # Domain Name
                lease_info['domain_name'] = value.decode('ascii', errors='ignore')
            i += 2 + length

        logging.debug(f"Parsed response: {lease_info}")
        return lease_info

    def request_lease(self):
        """Отправляет DISCOVER, ждёт OFFER, отправляет REQUEST, ждёт ACK."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(('0.0.0.0', 68))
        sock.settimeout(5)

        # DISCOVER
        discover_packet = self.build_packet(1)  # DHCPDISCOVER
        sock.sendto(discover_packet, ('255.255.255.255', 67))
        logging.info(f"Sent DHCPDISCOVER for MAC {self.mac_addr}, hostname {self.hostname}")

        # Ждём OFFER
        try:
            data, addr = sock.recvfrom(1024)
            offer = self.parse_response(data)
            if not offer or offer['msg_type'] != 2:  # DHCPOFFER
                logging.error("No valid OFFER received")
                return None
            logging.info(f"Received OFFER: IP {offer['ip']} from {offer['server_ip']}")

            # REQUEST
            request_packet = self.build_packet(3, offer['ip'])  # DHCPREQUEST
            sock.sendto(request_packet, ('255.255.255.255', 67))
            logging.info(f"Sent DHCPREQUEST for IP {offer['ip']}")

            # Ждём ACK
            data, addr = sock.recvfrom(1024)
            ack = self.parse_response(data)
            if not ack or ack['msg_type'] != 5:  # DHCPACK
                logging.error("No valid ACK received")
                return None
            logging.info(f"Received DHCPACK: IP {ack['ip']}")

            return ack
        except socket.timeout:
            logging.error("Timeout waiting for response")
            return None
        finally:
            sock.close()

    def print_lease_info(self, lease):
        """Выводит подробную информацию о полученной аренде."""
        if not lease:
            print("Failed to obtain lease.")
            return
        print("\n=== DHCP Lease Information ===")
        print(f"Assigned IP: {lease['ip']}")
        print(f"Server IP: {lease['server_ip']}")
        print(f"Subnet Mask: {lease['subnet_mask']}")
        print(f"Gateway: {lease['gateway']}")
        print(f"DNS Servers: {', '.join(lease['dns_servers'])}")
        print(f"Lease Time: {lease['lease_time']} seconds")
        print(f"Domain Name: {lease['domain_name']}")
        print(f"Message Type: {['', 'DISCOVER', 'OFFER', 'REQUEST', 'ACK'][lease['msg_type']-1]}")
        print("==============================\n")

def main():
    parser = argparse.ArgumentParser(description="DHCP Client for testing")
    parser.add_argument('--mac', help="MAC address (aa:bb:cc:dd:ee:ff)")
    parser.add_argument('--hostname', help="Hostname for the client")
    args = parser.parse_args()

    client = DHCPClient(args.mac, args.hostname)
    lease = client.request_lease()
    client.print_lease_info(lease)

if __name__ == "__main__":
    main()
