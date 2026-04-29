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

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

class StructuredJSONFormatter(logging.Formatter):
    def format(self, record):
        dt = datetime.fromtimestamp(record.created)
        timestamp = dt.strftime('%Y-%m-%d %H:%M:%S') + f'.{int(dt.microsecond / 1000):03d}'
        log_data = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "message": record.getMessage(),
            "line": record.lineno,
            "thread": record.threadName
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data, ensure_ascii=False)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'proxy_config.json')

def load_config():
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Ошибка загрузки конфига: proxy_config.json не найден в {BASE_DIR}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Ошибка загрузки конфига: Некорректный JSON: {e}")
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

    opensearch = config.get("opensearch", {})
    
    return {
        "servers": servers,
        "api_token": proxy.get("api_token", "your_token_here"),
        "proxy_port": proxy.get("proxy_port", 5501),
        "cache_ttl": proxy.get("cache_ttl", 30),
        "duplicate_mac_policy": proxy.get("duplicate_mac_policy", "keep_all").lower(),
        "dhcp_timeout_seconds": float(proxy.get("dhcp_timeout_seconds", 3)),
        "log_file": os.path.join(BASE_DIR, logging_cfg.get("log_file", "dhcp_proxy.log")),
        "log_level": logging_cfg.get("log_level", "INFO"),
        "max_log_size_mb": logging_cfg.get("max_log_size_mb", 10),
        "max_log_backup_count": logging_cfg.get("max_log_backup_count", 10),
        "os_send_enabled": opensearch.get("os_send_enabled", False),
        "os_urls": opensearch.get("os_urls", "http://127.0.0.1:9200"),
        "os_index": opensearch.get("os_index", "eventuro-dhcp-proxy-logs"),
        "os_flush_interval": opensearch.get("os_flush_interval", 5),
    }

cfg = load_config()

formatter = CustomFormatter(fmt='%(asctime)s [%(levelname)s] [%(name)s] {%(funcName)s}: %(message)s')

file_handler = RotatingFileHandler(
    cfg['log_file'],
    mode='a',
    maxBytes=cfg['max_log_size_mb'] * 1024 * 1024,
    backupCount=cfg['max_log_backup_count'],
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

# OpenSearch логгер
if cfg.get("os_send_enabled", False):
    try:
        from opensearch_logger import OpenSearchHandler
        
        es_handler = OpenSearchHandler(
            hosts=cfg["os_urls"],
            http_compress=True,
            use_ssl=False,
            index_name=cfg["os_index"],
            index_rotate=None,
            flush_frequency_in_sec=cfg["os_flush_interval"]
        )
        es_handler.setFormatter(StructuredJSONFormatter())
        es_handler.setLevel(cfg["log_level"])
        logger.addHandler(es_handler)
        logging.getLogger("opensearch").setLevel(logging.WARNING)
        log.info(f"OpenSearch логирование запущено в индекс {cfg['os_index']}")
    except ImportError:
        log.warning("Логирование в OpenSearch отключено: Модуль opensearch_logger не найден")
    except Exception as e:
        log.error(f"Ошибка инициализации OpenSearchHandler: {e}")

log.info("DHCP API Proxy запущен")
log.info(f"Серверов: {len(cfg['servers'])}, таймаут DHCP: {cfg['dhcp_timeout_seconds']}с, TTL кэша: {cfg['cache_ttl']}с")

app = Flask(__name__)
_cache = {}
_cache_ts = {}
_cache_lock = threading.Lock()

# Логи веб-сервера на уровень WARNING
log = logging.getLogger('werkzeug')
log.setLevel(logging.WARNING)

def log_request(endpoint, request_headers, request_body, response_headers, response_body, response_status):
    log_message = (
        f"<< Получен запрос по {endpoint}\n"
        f"<< Address: {request.url}\n"
        f"<< Headers: {dict(request_headers)}\n"
        f"<< Body: {request_body}"
    )
    logging.info(log_message)

    log_message = (
        f">> Отправлен ответ по {endpoint}\n"
        f">> Status: {response_status}\n"
        f">> Headers: {dict(response_headers)}\n"
        f">> Body: {response_body}"
    )
    logging.info(log_message)

def log_dhcp_request(url: str, direction: str, status: int = None, body: dict = None):
    if direction == "request":
        log_message = (
            f"<< Отправлен запрос к DHCP по {url}"
        )
        logging.info(log_message)
    else:  # response
        log_message = (
            f">> Получен ответ от DHCP по {url}\n"
            f">> Status: {status}\n"
            f">> Body: {body}"
        )
        logging.info(log_message)

def get_cached(key: str):
    with _cache_lock:
        if key in _cache and (time.time() - _cache_ts.get(key, 0)) < cfg["cache_ttl"]:
            return _cache[key]  # (status, data)
        return None

def set_cached(key: str, status: int, data):
    with _cache_lock:
        _cache[key] = (status, data)
        _cache_ts[key] = time.time()

def query_server(server: dict, endpoint: str, extra_params: dict = None):
    target_host = server.get('api_domain') or server['host']
    port = server.get('port')
    if port:
        url = f"http://{target_host}:{port}{endpoint}"
    else:
        url = f"http://{target_host}{endpoint}"

    params = {"token": cfg["api_token"]}
    if extra_params:
        params.update(extra_params)

    log_dhcp_request(url, "request")

    try:
        r = requests.get(url, params=params, timeout=cfg["dhcp_timeout_seconds"])

        try:
            data = r.json() if r.text.strip() else None
        except json.JSONDecodeError:
            data = None
            log.error(f"Невалидный JSON от {target_host}{endpoint}: {r.text[:300]}")
        
        log_dhcp_request(url, "response", status=r.status_code, body=data)

        return {
            "success": True,
            "status_code": r.status_code,
            "data": data,
            "is_dhcp_cached": bool(data.get("is_cached", False)) if isinstance(data, dict) else False
        }
    
    except requests.Timeout:
        log_dhcp_request(url, "response", status=504, body={"error": "timeout"})
        return {"success": False, "error": "timeout"}
    except requests.RequestException as e:
        log_dhcp_request(url, "response", status=502, body={"error": str(e)})
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

@app.route('/api/client/<ip>', methods=['GET'])
def api_client(ip: str):
    if request.args.get('token') != cfg["api_token"]:
        response = {"error": "Unauthorized"}
        log_request(
            endpoint=f"/api/client/{ip}",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=response,
            response_status=401
        )
        return jsonify(response), 401

    mac = request.args.get('mac')
    if mac:
        mac = mac.lower().strip()
        cache_key = f"client:{ip}:{mac}"
    else:
        cache_key = f"client:{ip}"
    cached = get_cached(cache_key)

    if cached is not None:
        status, data = cached
        data = data.copy() if isinstance(data, dict) else data
        data["is_cached"] = True
        log_request(
            endpoint=f"/api/client/{ip}",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=data,
            response_status=status
        )
        return jsonify(data), status

    server = get_server_for_ip(ip)
    if not server:
        resp = {"error": "No DHCP server responsible for this IP subnet", "ip": ip}
        log_request(
            endpoint=f"/api/client/{ip}",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=resp,
            response_status=400
        )
        set_cached(cache_key, 400, resp)
        return jsonify(resp), 400

    extra_params = {"mac": mac} if mac else None
    result = query_server(server, f"/api/client/{ip}", extra_params)

    if not result["success"]:
        err = result["error"]
        log.warning(f"DHCP {server['host']} не ответил на /client/{ip}: {err}")
        if err == "timeout":
            resp = {"error": "DHCP server timeout"}
            log_request(
                endpoint=f"/api/client/{ip}",
                request_headers=request.headers,
                request_body=request.get_data(as_text=True) or "No body",
                response_headers={'Content-Type': 'application/json'},
                response_body=resp,
                response_status=504
            )
            set_cached(cache_key, 504, resp)
            return jsonify(resp), 504
        resp = {"error": "DHCP server unavailable", "details": err}
        log_request(
            endpoint=f"/api/client/{ip}",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=resp,
            response_status=502
        )
        set_cached(cache_key, 502, resp)
        return jsonify(resp), 502

    # Прозрачное проксирование от DHCP
    status_code = result["status_code"]
    data = result["data"]

    resp = data.copy()
    resp.pop("source_server", None)
    resp["is_proxy"] = True
    resp["is_cached"] = False
    resp["is_dhcp_cached"] = result["is_dhcp_cached"]
    resp["source_server"] = server['host']
    # if mac: resp["requested_mac"] = mac

    log_request(
        endpoint=f"/api/client/{ip}",
        request_headers=request.headers,
        request_body=request.get_data(as_text=True) or "No body",
        response_headers={'Content-Type': 'application/json'},
        response_body=resp,
        response_status=status_code
    )
    set_cached(cache_key, status_code, resp)
    return jsonify(resp), status_code

@app.route('/api/clients', methods=['GET'])
def api_clients():
    if request.args.get('token') != cfg["api_token"]:
        response = {"error": "Unauthorized"}
        log_request(
            endpoint="/api/clients",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=response,
            response_status=401
        )
        return jsonify(response), 401

    cache_key = "all_clients"
    cached = get_cached(cache_key)
    if cached is not None:
        status, data = cached
        data = data.copy() if isinstance(data, dict) else data
        data["is_cached"] = True
        log_request(
            endpoint="/api/clients",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=data,
            response_status=status
        )
        return jsonify(data), status

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
                    log.warning(f"Сервер {srv['host']} -> ошибка: {resp['error']}")
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

    log_request(
        endpoint="/api/clients",
        request_headers=request.headers,
        request_body=request.get_data(as_text=True) or "No body",
        response_headers={'Content-Type': 'application/json'},
        response_body=result,
        response_status=200
    )
    set_cached(cache_key, 200, result)
    log.info(f"Список клиентов сформирован: {len(clients)} записей, ошибок серверов: {len(errors)}")
    return jsonify(result), 200

@app.route('/api/arp', methods=['GET'])
def api_arp():
    if request.args.get('token') != cfg["api_token"]:
        response = {"error": "Unauthorized"}
        log_request(
            endpoint="/api/arp",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=response,
            response_status=401
        )
        return jsonify(response), 401

    cache_key = "arp_table"
    cached = get_cached(cache_key)
    if cached is not None:
        status, data = cached
        data = data.copy() if isinstance(data, dict) else data
        data["is_cached"] = True
        log_request(
            endpoint="/api/arp",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=data,
            response_status=status
        )
        return jsonify(data), status

    errors = {}
    responses = []

    # Запрашиваем ARP-таблицу со всех серверов параллельно
    with ThreadPoolExecutor(max_workers=len(cfg["servers"])) as executor:
        future_to_server = {executor.submit(query_server, s, "/api/arp"): s for s in cfg["servers"]}
        for future in as_completed(future_to_server):
            srv = future_to_server[future]
            try:
                resp = future.result()
                if not resp["success"]:
                    errors[srv["host"]] = resp.get("error", "unknown error")
                    log.warning(f"Ошибка получения ARP с сервера {srv['host']} : {resp.get('error')}")
                else:
                    responses.append((srv, resp))
            except Exception as e:
                errors[srv["host"]] = str(e)
                log.error(f"Исключение при запросе ARP с сервера {srv['host']} : {e}")

    # Объединяем ARP-таблицы (оставляем только одну запись на IP)
    arp_entries = []
    seen_ips = set()

    for srv, resp in responses:
        if not resp.get("data") or "arp_table" not in resp["data"]:
            continue
        for entry in resp["data"].get("arp_table", []):
            ip = entry.get("ip")
            if not ip or ip in seen_ips:
                continue
            seen_ips.add(ip)
            
            entry = entry.copy()
            entry["source_server"] = srv["host"]
            arp_entries.append(entry)

    # Сортируем по IP
    arp_entries.sort(
        key=lambda x: tuple(map(int, x["ip"].split("."))) if x.get("ip") else (0, 0, 0, 0)
    )

    result = {
        "arp_table": arp_entries,
        "total": len(arp_entries),
        "is_proxy": True,
        "is_cached": False,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "errors": errors or None,
        "sources": [srv["host"] for srv, _ in responses]
    }

    log_request(
        endpoint="/api/arp",
        request_headers=request.headers,
        request_body=request.get_data(as_text=True) or "No body",
        response_headers={'Content-Type': 'application/json'},
        response_body=result,
        response_status=200
    )
    set_cached(cache_key, 200, result)

    log.info(f"ARP-таблица сформирована через прокси: {len(arp_entries)} записей (с {len(responses)} серверов)")
    return jsonify(result), 200

@app.route('/health')
def health():
    if request.args.get('token') and request.args.get('token') != cfg["api_token"]:
        response = {"error": "Unauthorized"}
        log_request(
            endpoint="/health",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=response,
            response_status=401
        )
        return jsonify(response), 401
    
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

    result = {
        "status": "ok" if alive > 0 else "degraded",
        "proxy_port": cfg["proxy_port"],
        "servers_total": len(cfg["servers"]),
        "servers_alive": alive,
        "servers_status": details,
        "dhcp_timeout": cfg["dhcp_timeout_seconds"],
        "cache_ttl": cfg["cache_ttl"],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    log_request(
        endpoint="/health",
        request_headers=request.headers,
        request_body=request.get_data(as_text=True) or "No body",
        response_headers={'Content-Type': 'application/json'},
        response_body=result,
        response_status=200
    )
    return jsonify(result), 200

if __name__ == '__main__':
    log.info(f"DHCP API Proxy стартует на порту {cfg['proxy_port']}")
    log.info(f"Подключено серверов: {[s['host'] for s in cfg['servers']]}")
    app.run(host="0.0.0.0", port=cfg["proxy_port"], threaded=True)
