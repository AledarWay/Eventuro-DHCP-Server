from flask import Flask, request, render_template, redirect, url_for, jsonify, flash, session
from functools import wraps
import logging
import re
import socket
import struct
import math
import os
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

def log_request(endpoint, request_headers, request_body, response_headers, response_body):
    logging.info(f"<< Получен запрос в API по {endpoint}")
    logging.info(f"<< Address: {request.url}")
    logging.info(f"<< Headers: {dict(request_headers)}")
    logging.info(f"<< Body: {request_body}")
    logging.info(f">> Отправлен ответ в API по {endpoint}")
    logging.info(f">> Headers: {dict(response_headers)}")
    logging.info(f">> Body: {response_body}")

def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
        return dt.strftime('%d.%m.%Y %H:%M:%S')
    except ValueError:
        return date_str

def time_to_expiry(expire_at):
    if not expire_at:
        return "Бессрочная"
    try:
        expiry_dt = datetime.strptime(expire_at, '%Y-%m-%d %H:%M:%S.%f')
        now = datetime.now()
        if expiry_dt <= now:
            return "Истёк"
        delta = expiry_dt - now
        seconds = delta.total_seconds()
        if seconds < 60:
            return f"{int(seconds)} сек"
        minutes = seconds / 60
        if minutes < 60:
            return f"{int(minutes)} мин"
        hours = minutes / 60
        if hours < 24:
            return f"{int(hours)} ч"
        days = hours / 24
        if days < 30:
            return f"{int(days)} д"
        months = days / 30
        if months < 12:
            return f"{int(months)} мес"
        years = months / 12
        return f"{int(years)} лет"
    except ValueError:
        return None

def create_app(server, db_manager, auth_manager):
    app = Flask(__name__)
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.config['SECRET_KEY'] = os.urandom(24)

    # Инициализация кэша (словарь для хранения данных по IP клиента)
    api_cache = {}  # Формат: {client_ip: (response, creation_time)}
    cache_ttl = server.config.get('api_cache_ttl', 10)  # Время жизни кэша

    # Функция для очистки устаревших записей в кэше
    def clean_cache():
        current_time = datetime.now()
        expired_keys = [
            key for key, (_, creation_time) in api_cache.items()
            if (current_time - creation_time).total_seconds() > cache_ttl
        ]
        for key in expired_keys:
            del api_cache[key]

    # Добавляем поддержку max и min функций в Jinja2
    app.jinja_env.globals.update({
        'max': max,
        'min': min,
        'range': range
    })
    
    column_names = {
        'mac': 'MAC-адрес',
        'ip': 'IP-адрес',
        'hostname': 'Имя устройства',
        'created_at': 'Дата первого подключения',
        'updated_at': 'Дата обновления аренды',
        'expire_at': 'Дата истечения аренды',
        'time_to_expiry': 'Осталось',
        'is_expired': 'Статус аренды',
        'lease_type': 'Тип аренды',
        'is_blocked': 'Статус блокировки',
        'server_ip': 'IP сервера',
        'subnet_mask': 'Маска подсети',
        'pool_start': 'Начало пула',
        'pool_end': 'Конец пула',
        'gateway': 'Шлюз',
        'dns_servers': 'DNS-серверы',
        'lease_time': 'Время аренды (сек)',
        'domain_name': 'Доменное имя',
        'id': 'Номер клиента',
        'client_id': 'Идентификатор клиента',
        'create_channel': 'Канал создания',
        'deleted_at': 'Дата удаления',
        'is_custom_hostname': 'Имя отредактировано',
        'timestamp': 'Время',
        'name': 'Имя устройства',
        'action': 'Действие',
        'description': 'Описание',
        'trust_flag': 'Признак доверия'
    }

    create_channel_translations = {
        'DHCP_REQUEST': 'Входящий запрос аренды',
        'STATIC_LEASE': 'Добавление статической привязки'
    }

    action_names = {
        'LEASE_ISSUED': 'Выдача аренды',
        'LEASE_EXPIRED': 'Истечение аренды',
        'HOSTNAME_UPDATED': 'Обновление имени хоста',
        'STATIC_ASSIGNED': 'Назначение статического IP',
        'DYNAMIC_ASSIGNED': 'Перевод в динамическую аренду',
        'DEVICE_DELETED': 'Удаление устройства',
        'DEVICE_RESTORED': 'Восстановление устройства',
        'LEASE_RESET': 'Сброс аренды',
        'LEASE_RENEWED': 'Продление аренды',
        'DEVICE_BLOCKED': 'Блокировка устройства',
        'DEVICE_UNBLOCKED': 'Разблокировка устройства',
        'CLIENT_CREATE': 'Создание клиента',
        'DECLINE': 'Отклонение IP клиентом',
        'NAK': 'Отказ клиенту в IP',
        'LEASE_RELEASED': 'Освобождение аренды клиентом',
        'INFORM': 'Информационный запрос',
        'TRUST_CHANGED': 'Изменение доверенности',
    }
    
    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'logged_in' not in session:
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated_function

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if auth_manager.user_exists():
            if request.method == 'POST':
                username = request.form.get('username')
                password = request.form.get('password')
                password_hash = auth_manager.get_user(username)
                if password_hash and check_password_hash(password_hash, password):
                    session['logged_in'] = True
                    return redirect(url_for('index'))
                else:
                    flash("Неверный логин или пароль", "danger")
            return render_template('login.html', setup=False)
        else:
            if request.method == 'POST':
                username = request.form.get('username')
                password = request.form.get('password')
                confirm_password = request.form.get('confirm_password')
                if password != confirm_password:
                    flash("Пароли не совпадают", "danger")
                elif not username or not password:
                    flash("Логин и пароль обязательны", "danger")
                else:
                    password_hash = generate_password_hash(password)
                    auth_manager.create_user(username, password_hash)
                    session['logged_in'] = True
                    flash("Учётная запись создана. Вы вошли в систему.", "success")
                    return redirect(url_for('index'))
            return render_template('login.html', setup=True)

    @app.route('/logout', methods=['POST'])
    def logout():
        session.pop('logged_in', None)
        flash("Вы вышли из системы", "success")
        return redirect(url_for('login'))

    @app.route('/', methods=['GET'])
    @login_required
    def index():
        saved_per_page = request.args.get('per_page', '20')
        
        if saved_per_page == '20' and 'per_page' not in request.args:
            saved_per_page = session.get('index_per_page', '20')
        
        per_page = int(saved_per_page)
        
        page = int(request.args.get('page', 1))
        mac_filter = request.args.get('mac', '').lower()
        ip_filter = request.args.get('ip', '')
        hostname_filter = request.args.get('hostname', '')
        lease_type_filter = request.args.get('lease_type', '')
        status_filter = request.args.get('status', '')
        sort_by = request.args.get('sort_by', 'ip')
        sort_order = request.args.get('sort_order', 'desc')

        valid_columns = ['mac', 'ip', 'hostname', 'is_expired', 'lease_type', 'expire_at']
        if sort_by not in valid_columns:
            sort_by = 'ip'

        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT id, mac, ip, hostname, created_at, updated_at, expire_at, is_expired, lease_type, is_blocked, client_id, create_channel, is_custom_hostname, trust_flag FROM leases WHERE deleted_at IS NULL"
            params = []
            if mac_filter:
                query += " AND mac LIKE ?"
                params.append(f"%{mac_filter}%")
            if ip_filter:
                query += " AND ip LIKE ?"
                params.append(f"%{ip_filter}%")
            if hostname_filter:
                query += " AND hostname LIKE ?"
                params.append(f"%{hostname_filter}%")
            if lease_type_filter:
                query += " AND lease_type = ?"
                params.append(lease_type_filter)
            if status_filter:
                query += " AND is_expired = ?"
                params.append(1 if status_filter == 'EXPIRED' else 0)

            # Базовая сортировка по умолчанию
            if sort_by != 'ip':
                query += f" ORDER BY {sort_by} {sort_order.upper()}"

            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            all_rows = cursor.fetchall()
            
            if sort_by == 'ip':
                def ip_sort_key(row):
                    ip = row[columns.index('ip')]
                    if ip:
                        try:
                            # Преобразуем IP в кортеж чисел для правильной сортировки
                            parts = ip.split('.')
                            if len(parts) == 4:
                                return tuple(int(part) for part in parts)
                        except ValueError:
                            pass
                    return (0, 0, 0, 0)  # Для NULL или некорректных IP
            
                if sort_order == 'asc':
                    all_rows.sort(key=ip_sort_key)
                else:
                    all_rows.sort(key=ip_sort_key, reverse=True)

            total_leases = len(all_rows)
            total_pages = math.ceil(total_leases / per_page)
            page = max(1, min(page, total_pages))
            start_index = (page - 1) * per_page
            end_index = min(start_index + per_page, total_leases)
            rows = all_rows[start_index:end_index]

            cursor.execute("SELECT COUNT(*) FROM leases WHERE deleted_at IS NULL")
            occupied = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM leases WHERE lease_type = 'STATIC' AND deleted_at IS NULL")
            static_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM leases WHERE lease_type = 'DYNAMIC' AND deleted_at IS NULL")
            dynamic_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM leases WHERE is_blocked = 1 AND deleted_at IS NULL")
            blocked_count = cursor.fetchone()[0]
            pool_start_int = ip_to_int(server.config['pool_start'])
            pool_end_int = ip_to_int(server.config['pool_end'])
            total_pool = pool_end_int - pool_start_int + 1
            free = total_pool - occupied

            formatted_rows = []
            lease_histories = []
            for row in rows:
                formatted_row = []
                for i, col in enumerate(row):
                    if columns[i] in ['created_at', 'updated_at', 'expire_at']:
                        formatted_row.append(format_date(col) or "Отсутствует")
                    elif columns[i] == 'is_expired':
                        formatted_row.append('Неактивно' if col else 'Активно')
                    elif columns[i] == 'lease_type':
                        formatted_row.append('Статическая' if col == 'STATIC' else 'Динамическая')
                    elif columns[i] == 'is_blocked':
                        formatted_row.append('Заблокировано' if col else 'Разрешено')
                    elif columns[i] == 'create_channel':
                        formatted_row.append(create_channel_translations.get(col, col if col is not None else '-'))
                    elif columns[i] in ['id', 'client_id']:
                        formatted_row.append(col if col is not None else '-')
                    elif columns[i] == 'is_custom_hostname':
                        formatted_row.append('Да' if col else 'Нет')
                    elif columns[i] == 'trust_flag':
                        formatted_row.append('Присутствует' if col else 'Отсутствует')
                    else:
                        formatted_row.append(col if col is not None else '-')
                formatted_row.append(time_to_expiry(row[columns.index('expire_at')]))
                formatted_rows.append(formatted_row)
                history = db_manager.get_lease_history(row[1], server.config['web_lease_history_limit'])
                formatted_history = []
                for hist in history:
                    formatted_history.append({
                        'id': hist[0],
                        'action': action_names.get(hist[1], hist[1]),
                        'ip': hist[2] or '-',
                        'new_ip': hist[3] or '-',
                        'name': hist[4] or '-',
                        'new_name': hist[5] or '-',
                        'description': hist[6],
                        'timestamp': format_date(hist[7]) or "Отсутствует"
                    })
                lease_histories.append(formatted_history)

            # Сохраняем per_page в сессию для следующего запроса
            session['index_per_page'] = str(per_page)

            return render_template('index.html', 
                                rows=formatted_rows, 
                                columns=columns,
                                extra_columns=['time_to_expiry'],
                                column_names=column_names,
                                action_names=action_names,
                                lease_histories=lease_histories,
                                page=page, 
                                per_page=per_page, 
                                total_pages=total_pages, 
                                total_leases=total_leases, 
                                start_index=start_index + 1, 
                                end_index=end_index,
                                mac_filter=mac_filter,
                                ip_filter=ip_filter,
                                hostname_filter=hostname_filter,
                                lease_type_filter=lease_type_filter,
                                status_filter=status_filter,
                                sort_by=sort_by,
                                sort_order=sort_order,
                                config=server.config,
                                stats={
                                    'occupied': occupied,
                                    'free': free,
                                    'static': static_count,
                                    'dynamic': dynamic_count,
                                    'blocked': blocked_count
                                })

    @app.route('/history', methods=['GET'])
    @login_required
    def history():
        saved_per_page = request.args.get('per_page', '20')
        
        if saved_per_page == '20' and 'per_page' not in request.args:
            saved_per_page = session.get('history_per_page', '20')
        
        per_page = int(saved_per_page)
        
        page = int(request.args.get('page', 1))
        mac_filter = request.args.get('mac', '').lower()
        action_filter = request.args.get('action', '')
        timestamp_filter = request.args.get('timestamp', '')
        sort_by = request.args.get('sort_by', 'timestamp')
        sort_order = request.args.get('sort_order', 'desc')

        valid_columns = ['timestamp', 'action', 'name']
        if sort_by not in valid_columns:
            sort_by = 'timestamp'
        if sort_order not in ['asc', 'desc']:
            sort_order = 'desc'

        with db_manager.get_history_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT timestamp, mac, name, action, description FROM lease_history WHERE 1=1"
            params = []
            if mac_filter:
                query += " AND mac LIKE ?"
                params.append(f"%{mac_filter}%")
            if action_filter:
                query += " AND action = ?"
                params.append(action_filter)
            if timestamp_filter:
                try:
                    dt = datetime.strptime(timestamp_filter, '%Y-%m-%d')
                    query += " AND date(timestamp) = ?"
                    params.append(dt.strftime('%Y-%m-%d'))
                except ValueError:
                    pass

            query += f" ORDER BY {sort_by} {sort_order.upper()}"

            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            all_rows = cursor.fetchall()
            total_history = len(all_rows)
            total_pages = math.ceil(total_history / per_page)
            page = max(1, min(page, total_pages))
            start_index = (page - 1) * per_page
            end_index = min(start_index + per_page, total_history)
            rows = all_rows[start_index:end_index]

            formatted_rows = []
            for row in rows:
                formatted_row = []
                for i, col in enumerate(row):
                    if columns[i] == 'timestamp':
                        formatted_row.append(format_date(col) or "Отсутствует")
                    elif columns[i] == 'action':
                        formatted_row.append(action_names.get(col, col))
                    else:
                        formatted_row.append(col if col is not None else '-')
                formatted_rows.append(formatted_row)

            cursor.execute("SELECT DISTINCT mac, name FROM lease_history ORDER BY mac")
            clients_list = cursor.fetchall()

            # Сохраняем per_page в сессию для следующего запроса
            session['history_per_page'] = str(per_page)

            return render_template('history.html',
                                rows=formatted_rows,
                                columns=columns,
                                column_names=column_names,
                                action_names=action_names,
                                page=page,
                                per_page=per_page,
                                total_pages=total_pages,
                                total_history=total_history,
                                start_index=start_index + 1,
                                end_index=end_index,
                                mac_filter=mac_filter,
                                action_filter=action_filter,
                                timestamp_filter=timestamp_filter,
                                sort_by=sort_by,
                                sort_order=sort_order,
                                clients_list=clients_list)

    @app.route('/block_device', methods=['POST'])
    @login_required
    def block_device():
        mac = request.form['mac'].lower()
        if not is_valid_mac(mac):
            flash("Неверный формат MAC-адреса", "danger")
            return redirect(url_for('index'))
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                db_manager.block_device(mac)
                flash(f"Устройство с MAC {mac} заблокировано", "success")
            else:
                flash(f"Устройство с MAC {mac} не найдено", "danger")
        return redirect(url_for('index'))

    @app.route('/set_trust', methods=['POST'])
    @login_required
    def set_trust():
        mac = request.form['mac'].lower()
        trust_flag = int(request.form['trust_flag'])
        if not is_valid_mac(mac):
            flash("Неверный формат MAC-адреса", "danger")
            return redirect(url_for('index'))
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                success = db_manager.set_trust_flag(mac, trust_flag, change_channel='WEB')
                if success:
                    status = "доверенным" if trust_flag == 1 else "недоверенным"
                    flash(f"Устройство с MAC {mac} признано {status}", "success")
                else:
                    flash(f"Не удалось изменить статус доверенности для MAC {mac}", "danger")
            else:
                flash(f"Устройство с MAC {mac} не найдено", "danger")
        return redirect(url_for('index'))

    @app.route('/unblock_device', methods=['POST'])
    @login_required
    def unblock_device():
        mac = request.form['mac'].lower()
        if not is_valid_mac(mac):
            flash("Неверный формат MAC-адреса", "danger")
            return redirect(url_for('index'))
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                db_manager.unblock_device(mac)
                flash(f"Устройство с MAC {mac} разблокировано", "success")
            else:
                flash(f"Устройство с MAC {mac} не найдено", "danger")
        return redirect(url_for('index'))

    @app.route('/update_hostname', methods=['POST'])
    @login_required
    def update_hostname():
        mac = request.form['mac'].lower()
        hostname = request.form['hostname'].strip()
        if not hostname:
            flash("Имя хоста не может быть пустым", "danger")
            return redirect(url_for('index'))
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                current_ip, current_hostname = row
                if current_hostname != hostname:
                    db_manager.update_hostname(mac, hostname, change_channel='WEB')
                    flash(f"Имя хоста для MAC {mac} обновлено на {hostname}", "success")
                else:
                    flash(f"Имя хоста для MAC {mac} не изменилось", "info")
            else:
                flash(f"Аренда для MAC {mac} не найдена", "danger")
        return redirect(url_for('index'))

    @app.route('/reset_hostname_manual', methods=['POST'])
    @login_required
    def reset_hostname_manual():
        mac = request.form['mac'].lower()
        if not is_valid_mac(mac):
            flash("Неверный формат MAC-адреса", "danger")
            return redirect(url_for('index'))
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if not row:
                flash(f"Аренда для MAC {mac} не найдена", "danger")
                return redirect(url_for('index'))
            current_ip, current_hostname = row
        with db_manager.get_history_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM lease_history WHERE mac = ? AND action = 'CLIENT_CREATE' ORDER BY timestamp ASC LIMIT 1", (mac,))
            history_row = cursor.fetchone()
            new_hostname = history_row[0] if history_row and history_row[0] else None
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            if current_hostname == new_hostname:
                cursor.execute("UPDATE leases SET is_custom_hostname = 0 WHERE mac = ? AND deleted_at IS NULL", (mac,))
                conn.commit()
                flash(f"Ручное имя хоста для MAC {mac} сброшено", "success")
            else:
                cursor.execute("UPDATE leases SET hostname = ?, is_custom_hostname = 0 WHERE mac = ? AND deleted_at IS NULL", (new_hostname, mac))
                conn.commit()
        with db_manager.get_history_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            db_manager._insert_history(
                mac=mac,
                action='HOSTNAME_UPDATED',
                ip=current_ip,
                new_ip=None,
                name=current_hostname or 'не указано',
                new_name=new_hostname or 'не указано',
                description=f"Имя хоста сброшено с '{current_hostname or 'не указано'}' на '{new_hostname or 'не указано'}'",
                timestamp=current_time,
                change_channel='WEB'
            )
            conn.commit()
            flash(f"Имя хоста для MAC {mac} сброшено на '{new_hostname or 'не указано'}'", "success")
        return redirect(url_for('index'))
    
    @app.route('/set_static', methods=['POST'])
    @login_required
    def set_static():
        mac = request.form['mac'].lower()
        ip = request.form['ip'].strip()
        if not is_valid_mac(mac):
            flash("Неверный формат MAC-адреса (используйте aa:bb:cc:dd:ee:ff)", "danger")
            return redirect(url_for('index'))
        if not is_valid_ip(ip):
            flash("Неверный формат IP-адреса", "danger")
            return redirect(url_for('index'))
        start_ip_int, end_ip_int = get_subnet_range(server.config['server_ip'], server.config['subnet_mask'])
        ip_int = ip_to_int(ip)
        if not (start_ip_int <= ip_int <= end_ip_int):
            flash("IP-адрес вне диапазона подсети (x.x.x.2 - x.x.x.254)", "danger")
            return redirect(url_for('index'))
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT mac, ip, hostname, lease_type FROM leases WHERE ip = ? AND mac != ? AND deleted_at IS NULL", (ip, mac))
            if cursor.fetchone():
                flash("IP-адрес уже используется", "danger")
                return redirect(url_for('index'))
            cursor.execute("SELECT ip, hostname, lease_type FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                old_ip, hostname, lease_type = row
                if lease_type == 'STATIC':
                    flash(f"MAC {mac} уже имеет статическую аренду", "danger")
                    return redirect(url_for('index'))
                db_manager.update_ip(mac, ip, change_channel='WEB')
                db_manager.update_lease_type(mac, 'STATIC', change_channel='WEB')
                flash(f"Статическая аренда установлена для MAC {mac}: IP {ip}", "success")
            else:
                flash(f"Аренда для MAC {mac} не найдена", "danger")
            conn.commit()
        return redirect(url_for('index'))

    @app.route('/add_static', methods=['POST'])
    @login_required
    def add_static():
        bulk_data = request.form.get('bulk_data', '').strip()
        single_mac = request.form.get('mac', '').lower().strip()
        single_ip = request.form.get('ip', '').strip()
        single_hostname = request.form.get('hostname', '').strip() or None
        start_ip_int, end_ip_int = get_subnet_range(server.config['server_ip'], server.config['subnet_mask'])

        if single_mac and single_ip:
            # Единичное добавление
            if not is_valid_mac(single_mac):
                flash("Неверный формат MAC-адреса (используйте aa:bb:cc:dd:ee:ff)", "danger")
                return redirect(url_for('index'))
            if not is_valid_ip(single_ip):
                flash("Неверный формат IP-адреса", "danger")
                return redirect(url_for('index'))
            ip_int = ip_to_int(single_ip)
            if not (start_ip_int <= ip_int <= end_ip_int):
                flash("IP-адрес вне диапазона подсети (x.x.x.2 - x.x.x.254)", "danger")
                return redirect(url_for('index'))
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT mac, lease_type, ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (single_mac,))
                existing = cursor.fetchone()
                if existing:
                    if existing[1] == 'STATIC':
                        flash(f"MAC {single_mac} уже имеет статическую аренду", "danger")
                        return redirect(url_for('index'))
                    old_ip, old_hostname = existing[2], existing[3]
                    cursor.execute("SELECT mac FROM leases WHERE ip = ? AND mac != ? AND deleted_at IS NULL", (single_ip, single_mac))
                    if cursor.fetchone():
                        flash(f"IP-адрес {single_ip} уже используется", "danger")
                        return redirect(url_for('index'))
                    db_manager.update_ip(single_mac, single_ip, change_channel='WEB')
                    if single_hostname and single_hostname != old_hostname:
                        db_manager.update_hostname(single_mac, single_hostname, change_channel='WEB')
                    db_manager.update_lease_type(single_mac, 'STATIC', change_channel='WEB')
                    flash(f"Статическая аренда установлена для MAC {single_mac}: IP {single_ip}, имя хоста {single_hostname or old_hostname or 'не указано'}", "success")
                else:
                    cursor.execute("SELECT mac FROM leases WHERE ip = ? AND deleted_at IS NULL", (single_ip,))
                    if cursor.fetchone():
                        flash(f"IP-адрес {single_ip} уже используется", "danger")
                        return redirect(url_for('index'))
                    db_manager.create_lease(single_mac, single_ip, single_hostname, lease_type='STATIC', create_channel='STATIC_LEASE', change_channel='WEB')
                    flash(f"Добавлена статическая аренда: MAC {single_mac}, IP {single_ip}, имя хоста {single_hostname or 'не указано'}", "success")
                conn.commit()
        elif bulk_data:
            # Массовое добавление
            errors = []
            successes = []
            for line in bulk_data.split('\n'):
                parts = line.strip().split(';')
                if len(parts) < 2 or len(parts) > 3:
                    errors.append(f"Неверный формат строки: {line}")
                    continue
                mac, ip = parts[0].lower().strip(), parts[1].strip()
                hostname = parts[2].strip() or None if len(parts) == 3 else None
                if not is_valid_mac(mac):
                    errors.append(f"Неверный MAC в строке: {line}")
                    continue
                if not is_valid_ip(ip):
                    errors.append(f"Неверный IP в строке: {line}")
                    continue
                ip_int = ip_to_int(ip)
                if not (start_ip_int <= ip_int <= end_ip_int):
                    errors.append(f"IP {ip} вне диапазона подсети в строке: {line}")
                    continue
                with db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT mac, lease_type, ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
                    existing = cursor.fetchone()
                    if existing:
                        if existing[1] == 'STATIC':
                            errors.append(f"MAC {mac} уже имеет статическую аренду в строке: {line}")
                            continue
                        old_ip, old_hostname = existing[2], existing[3]
                        cursor.execute("SELECT mac FROM leases WHERE ip = ? AND mac != ? AND deleted_at IS NULL", (ip, mac))
                        if cursor.fetchone():
                            errors.append(f"IP {ip} уже используется в строке: {line}")
                            continue
                        db_manager.update_ip(mac, ip, change_channel='WEB')
                        if hostname and hostname != old_hostname:
                            db_manager.update_hostname(mac, hostname, change_channel='WEB')
                        db_manager.update_lease_type(mac, 'STATIC', change_channel='WEB')
                        successes.append(f"MAC {mac}, IP {ip}, имя хоста {hostname or old_hostname or 'не указано'}")
                    else:
                        cursor.execute("SELECT mac FROM leases WHERE ip = ? AND deleted_at IS NULL", (ip,))
                        if cursor.fetchone():
                            errors.append(f"IP {ip} уже используется в строке: {line}")
                            continue
                        db_manager.create_lease(mac, ip, hostname, lease_type='STATIC', create_channel='STATIC_LEASE', change_channel='WEB')
                        successes.append(f"MAC {mac}, IP {ip}, имя хоста {hostname or 'не указано'}")
                    conn.commit()
            if successes:
                flash("Добавлены статические аренды: " + "; ".join(successes), "success")
            if errors:
                flash("Ошибки при добавлении: " + "; ".join(errors), "danger")
        else:
            flash("Не указаны данные для добавления", "danger")
        return redirect(url_for('index'))

    @app.route('/set_dynamic', methods=['POST'])
    @login_required
    def set_dynamic():
        mac = request.form['mac'].lower()
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                db_manager.update_lease_type(mac, 'DYNAMIC', change_channel='WEB')
                flash(f"Аренда для MAC {mac} переведена в динамическую", "success")
            else:
                flash(f"Аренда для MAC {mac} не найдена", "danger")
            conn.commit()
        return redirect(url_for('index'))

    @app.route('/reset_lease', methods=['POST'])
    @login_required
    def reset_lease():
        mac = request.form['mac'].lower()
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                old_ip, hostname = row
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                if old_ip:
                    cursor.execute("UPDATE leases SET is_expired = 1, updated_at = ?, expire_at = ?, ip = null WHERE mac = ? AND deleted_at IS NULL",
                                (current_time, current_time, mac))
                    db_manager._insert_history(mac, 'LEASE_RESET', old_ip, None, hostname or 'не указано', None,
                                            f"Аренда сброшена, IP {old_ip} освобождён",
                                            current_time, change_channel='WEB')
                    db_manager.update_lease_type(mac, 'DYNAMIC', change_channel='WEB')
                    flash(f"Аренда для MAC {mac} сброшена", "success")
                else:
                    flash(f"Аренда для MAC {mac} уже неактивна", "danger")
            else:
                flash(f"Аренда для MAC {mac} не найдена", "danger")
            conn.commit()
        return redirect(url_for('index'))

    @app.route('/delete', methods=['POST'])
    @login_required
    def delete():
        mac = request.form['mac'].lower()
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ip, hostname, is_expired FROM leases WHERE mac = ? AND deleted_at IS NULL", (mac,))
            row = cursor.fetchone()
            if row:
                ip, hostname, is_expired = row
                if ip is not None or is_expired != 1:
                    flash(f"Удаление возможно только для клиентов без IP и с истёкшей арендой", "danger")
                    return redirect(url_for('index'))
                db_manager.delete(mac)
                flash(f"Устройство {mac} удалено", "success")
            else:
                flash(f"Устройство MAC {mac} не найдено", "danger")
        return redirect(url_for('index'))
    
    @app.route('/logs', methods=['GET'])
    @login_required
    def logs():
        try:
            with open(server.config['log_file'], 'r', encoding='utf-8', errors='replace') as f:
                logs = f.read()
            return jsonify({'logs': logs})
        except UnicodeDecodeError as ude:
            logging.error(f"Error decoding logs file {server.config['log_file']}: {ude}")
            return jsonify({'error': 'Ошибка декодирования файла логов.'}), 500
        except FileNotFoundError:
            logging.error(f"Log file not found: {server.config['log_file']}")
            return jsonify({'error': 'Файл логов не найден.'}), 500
        except PermissionError:
            logging.error(f"Permission denied accessing log file: {server.config['log_file']}")
            return jsonify({'error': 'Нет прав доступа к файлу логов.'}), 500
        except Exception as e:
            logging.error(f"Unexpected error reading logs from {server.config['log_file']}: {e}")
            return jsonify({'error': 'Произошла непредвиденная ошибка при чтении логов.'}), 500

    @app.route('/api/client/<ip>', methods=['GET'])
    def get_client_info(ip):
        token = request.args.get('token')
        client_ip = request.remote_addr

        if 'api_token' not in server.config or token != server.config['api_token']:
            response = {'error': 'Unauthorized'}
            log_request(
                endpoint=f"/api/client/{ip}",
                request_headers=request.headers,
                request_body=request.get_data(as_text=True) or "No body",
                response_headers={'Content-Type': 'application/json'},
                response_body=response
            )
            return jsonify(response), 401

        # Очищаем устаревшие записи в кэше
        clean_cache()

        # Используем запрошенный IP как ключ кэша
        cache_key = ip
        is_cached = False
        if cache_key in api_cache:
            response, creation_time = api_cache[cache_key]
            if (datetime.now() - creation_time).total_seconds() <= cache_ttl:
                is_cached = True
                response['is_cached'] = True
                log_request(
                    endpoint=f"/api/client/{ip}",
                    request_headers=request.headers,
                    request_body=request.get_data(as_text=True) or "No body",
                    response_headers={'Content-Type': 'application/json'},
                    response_body=response
                )
                return jsonify(response)

        # Если нет в кэше или запись устарела, выполняем запрос к базе
        data = db_manager.get_client_by_ip(ip)
        if data:
            response = {
                'mac': data['mac'],
                'ip': data['ip'],
                'hostname': data['hostname'],
                'client_id': data['client_id'],
                'created_at': format_date(data['created_at']),
                'updated_at': format_date(data['updated_at']),
                'expire_at': format_date(data['expire_at']),
                'time_to_expiry': time_to_expiry(data['expire_at']),
                'is_expired': data['is_expired'],
                'lease_type': data['lease_type'],
                'is_blocked': data['is_blocked'],
                'is_custom_hostname': data['is_custom_hostname'],
                'trust_flag': data['trust_flag'],
                'is_cached': False
            }
            status_code = 200
        else:
            response = {'error': 'Client not found', 'is_cached': False}
            status_code = 404

        # Сохраняем результат в кэш
        api_cache[cache_key] = (response, datetime.now())

        # Логируем запрос и ответ
        log_request(
            endpoint=f"/api/client/{ip}",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=response
        )

        return jsonify(response), status_code

    @app.route('/api/clients', methods=['GET'])
    def get_all_clients():
        token = request.args.get('token')
        client_ip = request.remote_addr

        if 'api_token' not in server.config or token != server.config['api_token']:
            response = {'error': 'Unauthorized'}
            log_request(
                endpoint="/api/clients",
                request_headers=request.headers,
                request_body=request.get_data(as_text=True) or "No body",
                response_headers={'Content-Type': 'application/json'},
                response_body=response
            )
            return jsonify(response), 401

        # Очищаем устаревшие записи в кэше
        clean_cache()

        # Используем фиксированный ключ для кэша всех клиентов
        cache_key = "all_clients"
        is_cached = False
        if cache_key in api_cache:
            response, creation_time = api_cache[cache_key]
            if (datetime.now() - creation_time).total_seconds() <= cache_ttl:
                is_cached = True
                response['is_cached'] = True
                log_request(
                    endpoint="/api/clients",
                    request_headers=request.headers,
                    request_body=request.get_data(as_text=True) or "No body",
                    response_headers={'Content-Type': 'application/json'},
                    response_body=response
                )
                return jsonify(response)

        # Если нет в кэше или запись устарела, выполняем запрос к базе
        columns, rows = db_manager.get_all_leases()
        clients = []
        for row in rows:
            client_data = {}
            raw_expire_at = None
            for i, col in enumerate(row):
                col_name = columns[i]
                if col_name == 'expire_at':
                    raw_expire_at = col
                    client_data[col_name] = format_date(col)
                elif col_name in ['created_at', 'updated_at', 'deleted_at']:
                    client_data[col_name] = format_date(col)
                else:
                    client_data[col_name] = col if col is not None else None
            client_data['time_to_expiry'] = time_to_expiry(raw_expire_at)
            clients.append(client_data)

        response = {'clients': clients, 'total': len(clients), 'is_cached': False}

        # Сохраняем результат в кэш
        api_cache[cache_key] = (response, datetime.now())

        # Логируем запрос и ответ
        log_request(
            endpoint="/api/clients",
            request_headers=request.headers,
            request_body=request.get_data(as_text=True) or "No body",
            response_headers={'Content-Type': 'application/json'},
            response_body=response
        )

        return jsonify(response)

    return app

# Проверка конфигов
def validate_config(config):
    errors = []
    required_keys = ['server_ip', 'gateway', 'pool_start', 'pool_end', 'subnet_mask']
    for key in required_keys:
        if key not in config:
            errors.append(f"Missing {key}")
            continue
        if not is_valid_ip(config[key]):
            errors.append(f"Invalid {key}")
    if errors:
        return False, "; ".join(errors)
    
    mask_int = ip_to_int(config['subnet_mask'])
    if mask_int == 0 or mask_int == 0xFFFFFFFF:
        errors.append("Invalid subnet_mask range")
    
    pool_start_int = ip_to_int(config['pool_start'])
    pool_end_int = ip_to_int(config['pool_end'])
    if pool_start_int > pool_end_int:
        errors.append("pool_start > pool_end")
    
    subnet_base = config['server_ip']
    if not is_in_subnet(config['server_ip'], subnet_base, config['subnet_mask']):
        errors.append("server_ip not in subnet")
    if not is_in_subnet(config['gateway'], subnet_base, config['subnet_mask']):
        errors.append("gateway not in subnet")
    if not is_in_subnet(config['pool_start'], subnet_base, config['subnet_mask']):
        errors.append("pool_start not in subnet")
    if not is_in_subnet(config['pool_end'], subnet_base, config['subnet_mask']):
        errors.append("pool_end not in subnet")
    
    if errors:
        return False, "; ".join(errors)
    return True, None

def is_valid_ip(ip):
    try:
        socket.inet_aton(ip)
        return True
    except socket.error:
        return False

def is_valid_mac(mac):
    return bool(re.match(r'^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$', mac))

def ip_to_int(ip):
    return struct.unpack("!I", socket.inet_aton(ip))[0]

def is_in_subnet(ip, subnet_base, mask):
    mask_int = ip_to_int(mask)
    ip_int = ip_to_int(ip)
    subnet_int = ip_to_int(subnet_base) & mask_int
    return (ip_int & mask_int) == subnet_int

def get_subnet_range(subnet_base, mask):
    mask_int = ip_to_int(mask)
    network_int = ip_to_int(subnet_base) & mask_int
    start_ip_int = network_int + 2  # x.x.x.2
    end_ip_int = network_int | (~mask_int & 0xFFFFFFFF) - 1  # x.x.x.254
    return start_ip_int, end_ip_int