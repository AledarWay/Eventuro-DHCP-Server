#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import time
import threading
import ipaddress
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from flask import Flask, request, jsonify

# ======================== ЛОГИРОВАНИЕ ========================

class CustomRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.baseFilenameWithoutExt = filename.rsplit('.', 1)[0]

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        if self.backupCount > 0:
            import glob
            for i in range(self.backupCount - 1, 0, -1):
                sfn = f"{self.baseFilenameWithoutExt}_{i}_*.log"
                dfn = f"{self.baseFilenameWithoutExt}_{i + 1}_{current_time}.log"
                for old_file in glob.glob(sfn):
                    if os.path.exists(old_file):
                        os.rename(old_file, dfn)
            dfn = f"{self.baseFilenameWithoutExt}_1_{current_time}.log"
            self.rotate(self.baseFilename, dfn)
        if not self.delay:
            self.stream = self._open()

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# ======================== КОНФИГУРАЦИЯ ========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'proxy_config.json')

def load_config():
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"ОШИБКА: proxy_config.json не найден в {BASE_DIR}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"ОШИБКА: Некорректный JSON: {e}")
        exit(1)

    proxy = config.get("proxy", {})
    logging_cfg = config.get("logging", {})

    servers = proxy.get("servers", [])
    for srv in servers:
        host = srv["host"]
        try:
            net = ipaddress.ip_network(f"{host}/24", strict=False)
            srv["network"] = net
        except:
            srv["network"] = None

    return {
        "servers": servers,
        "api_token": proxy.get("api_token", "your_token_here"),
        "proxy_port": proxy.get("proxy_port", 5501),
        "cache_ttl": proxy.get("cache_ttl", 30),
        "duplicate_mac_policy": proxy.get("duplicate_mac_policy", "keep_all").lower(),
        "dhcp_timeout_seconds": float(proxy.get("dhcp_timeout_seconds", 3)),  # новое поле

        "log_file": os.path.join(BASE_DIR, logging_cfg.get("log_file", "dhcp_proxy.log")),
        "log_level": logging_cfg.get("log_level", "INFO"),
        "max_log_size_mb": logging_cfg.get("max_log_size_mb", 10),
        "max_log_backup_count": logging_cfg.get("max_log_backup_count", 10),
    }

cfg = load_config()

# ======================== ЛОГИРОВАНИЕ ========================

formatter = CustomFormatter(fmt='%(asctime)s [%(levelname)s] [%(name)s] {%(funcName)s}: %(message)s')

file_handler = CustomRotatingFileHandler(
    cfg["log_file"],
    maxBytes=cfg["max_log_size_mb"] * 1024 * 1024,
    backupCount=cfg["max_log_backup_count"],
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(cfg["log_level"])
logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

log = logging.getLogger(__name__)

log.info("DHCP API Proxy запущен (умный роутинг + настраиваемый таймаут)")
log.info(f"Серверов: {len(cfg['servers'])}, таймаут DHCP: {cfg['dhcp_timeout_seconds']}с, TTL кэша: {cfg['cache_ttl']}с")

# ======================== КЭШ ========================

app = Flask(__name__)
_cache = {}
_cache_ts = {}
_cache_lock = threading.Lock()

def get_cached(key: str):
    with _cache_lock:
        if key in _cache and (time.time() - _cache_ts.get(key, 0)) < cfg["cache_ttl"]:
            return _cache[key]
        return None

def set_cached(key: str, value):
    with _cache_lock:
        _cache[key] = value
        _cache_ts[key] = time.time()

# ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================

def query_server(server: dict, endpoint: str):
    url = f"http://{server['host']}:{server.get('port', 5500)}{endpoint}"
    params = {"token": cfg["api_token"]}
    try:
        r = requests.get(url, params=params, timeout=cfg["dhcp_timeout_seconds"])
        if r.status_code == 200:
            data = r.json()
            return {
                "success": True,
                "data": data,
                "is_dhcp_cached": bool(data.get("is_cached", False))
            }
        elif r.status_code == 404:
            return {"success": True, "data": None, "is_dhcp_cached": False}
        else:
            return {"success": False, "error": f"HTTP {r.status_code}"}
    except requests.Timeout:
        return {"success": False, "error": "timeout"}
    except requests.RequestException as e:
        return {"success": False, "error": str(e)}

def get_server_for_ip(ip: str):
    try:
        ip_obj = ipaddress.ip_address(ip)
        for srv in cfg["servers"]:
            net = srv.get("network")
            if net and ip_obj in net:
                return srv
        return None
    except:
        return None

def merge_clients(responses):
    clients = []
    for resp in responses:
        if not resp["success"] or not resp.get("data") or "clients" not in resp["data"]:
            continue
        for client in resp["data"]["clients"]:
            c = client.copy()
            c.pop("source_server", None)
            clients.append(c)

    clients.sort(key=lambda x: tuple(map(int, x["ip"].split("."))) if x.get("ip") else (0,0,0,0), reverse=True)

    policy = cfg["duplicate_mac_policy"]
    if policy == "merge":
        seen = set()
        unique = []
        for c in reversed(clients):
            mac = c.get("mac")
            if mac and mac not in seen:
                seen.add(mac)
                unique.append(c)
        clients = unique[::-1]
    elif policy == "prefer_ip":
        by_mac = {}
        for c in clients:
            mac = c.get("mac")
            if not mac: continue
            cur_time = c.get("expire_at") or c.get("updated_at") or ""
            existing = by_mac.get(mac, {})
            if cur_time > (existing.get("expire_at") or existing.get("updated_at") or ""):
                by_mac[mac] = c
        clients = list(by_mac.values())
        clients.sort(key=lambda x: tuple(map(int, x["ip"].split("."))) if x.get("ip") else (0,0,0,0), reverse=True)

    return clients

# ======================== API ========================

@app.route('/api/client/<ip>', methods=['GET'])
def api_client(ip: str):
    if request.args.get('token') != cfg["api_token"]:
        return jsonify({"error": "Unauthorized"}), 401

    cache_key = f"client:{ip}"
    cached = get_cached(cache_key)
    if cached is not None:
        cached["is_cached"] = True
        return jsonify(cached)

    server = get_server_for_ip(ip)
    if not server:
        resp = {"error": "No DHCP server responsible for this IP subnet", "ip": ip}
        set_cached(cache_key, resp)
        return jsonify(resp), 400

    log.info(f"Запрос клиента {ip} → сервер {server['host']} (таймаут {cfg['dhcp_timeout_seconds']}с)")

    result = query_server(server, f"/api/client/{ip}")

    if not result["success"]:
        err = result["error"]
        log.warning(f"DHCP {server['host']} не ответил на /client/{ip}: {err}")
        if err == "timeout":
            return jsonify({"error": "DHCP server timeout"}), 504
        return jsonify({"error": "DHCP server unavailable", "details": err}), 502

    data = result["data"]
    if data and data.get("ip") == ip:
        resp = data.copy()
        resp.pop("source_server", None)
        resp["is_proxy"] = True
        resp["is_cached"] = False
        resp["is_dhcp_cached"] = result["is_dhcp_cached"]
        resp["source_server"] = server['host']
        set_cached(cache_key, resp)
        return jsonify(resp), 200

    resp = {
        "error": "Client not found",
        "ip": ip,
        "is_proxy": True,
        "is_cached": False,
        "is_dhcp_cached": False
    }
    set_cached(cache_key, resp)
    return jsonify(resp), 404


@app.route('/api/clients', methods=['GET'])
def api_clients():
    if request.args.get('token') != cfg["api_token"]:
        return jsonify({"error": "Unauthorized"}), 401

    cache_key = "all_clients"
    cached = get_cached(cache_key)
    if cached is not None:
        cached["is_cached"] = True
        return jsonify(cached)

    errors = {}
    responses = []

    with ThreadPoolExecutor(max_workers=len(cfg["servers"])) as executor:
        future_to_server = {executor.submit(query_server, s, "/api/clients"): s for s in cfg["servers"]}
        for future in as_completed(future_to_server):
            srv = future_to_server[future]
            try:
                resp = future.result()
                if not resp["success"]:
                    errors[srv["host"]] = resp["error"]
                    log.warning(f"Сервер {srv['host']} → ошибка: {resp['error']}")
                else:
                    responses.append(resp)
            except Exception as e:
                errors[srv["host"]] = "exception"
                log.error(f"Исключение от {srv['host']}: {e}")

    clients = merge_clients(responses)

    result = {
        "clients": clients,
        "total": len(clients),
        "is_cached": False,
        "is_proxy": True,
        "is_dhcp_cached": [r.get("is_dhcp_cached", False) for r in responses],
        "duplicate_mac_policy": cfg["duplicate_mac_policy"],
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "errors": errors or None
    }

    set_cached(cache_key, result)
    log.info(f"Список клиентов сформирован: {len(clients)} записей, ошибок серверов: {len(errors)}")
    return jsonify(result)


@app.route('/health')
def health():
    alive = 0
    details = {}
    with ThreadPoolExecutor(max_workers=len(cfg["servers"])) as executor:
        futures = {executor.submit(query_server, s, "/health"): s for s in cfg["servers"]}
        for f in as_completed(futures):
            srv = futures[f]
            try:
                res = f.result(timeout=2)
                status = "ok" if res["success"] else res.get("error", "failed")
                details[srv["host"]] = status
                if res["success"]: alive += 1
            except:
                details[srv["host"]] = "timeout"
    return jsonify({
        "status": "ok" if alive > 0 else "degraded",
        "proxy_port": cfg["proxy_port"],
        "servers_total": len(cfg["servers"]),
        "servers_alive": alive,
        "servers_status": details,
        "dhcp_timeout": cfg["dhcp_timeout_seconds"],
        "cache_ttl": cfg["cache_ttl"],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# ======================== ЗАПУСК ========================

if __name__ == '__main__':
    log.info(f"DHCP API Proxy стартует на порту {cfg['proxy_port']}")
    log.info(f"Подключено серверов: {[s['host'] for s in cfg['servers']]}")
    app.run(host="0.0.0.0", port=cfg["proxy_port"], threaded=True)
