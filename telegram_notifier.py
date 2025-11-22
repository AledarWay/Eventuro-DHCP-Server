import requests
import logging
import time
from datetime import datetime, timedelta
import re

class TelegramNotifier:
    def __init__(self, config, db_manager):
        self.bot_token = config['telegram_bot_token']
        self.chat_id = config['telegram_chat_id']
        self.web_url = config['telegram_web_url']
        self.domain_name = config['domain_name']
        self.enabled = config.get('telegram_enabled', True)
        self.notify_new = config.get('telegram_notify_new_device', True)
        self.notify_inactive = config.get('telegram_notify_inactive_device', True)
        self.retries = config.get('telegram_retries', 3)
        self.retry_interval = config.get('telegram_retry_interval', 5)
        self.inactive_period = self.parse_duration(config.get('inactive_period', '7d'))
        self.thread_id = config.get('telegram_thread_id', None)
        self.db_manager = db_manager
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        logging.info("TelegramNotifier инициализирован с enabled=%s, notify_new=%s, notify_inactive=%s, retries=%s, retry_interval=%s, inactive_period=%s, thread_id=%s",
                     self.enabled, self.notify_new, self.notify_inactive, self.retries, self.retry_interval, self.inactive_period, self.thread_id)
    
    def parse_duration(self, s):
        try:
            num = int(s[:-1])
            unit = s[-1].lower()
            if unit == 'm':
                return timedelta(minutes=num)
            elif unit == 'h':
                return timedelta(hours=num)
            elif unit == 'd':
                return timedelta(days=num)
            elif unit == 'y':
                return timedelta(days=num * 365)
            else:
                raise ValueError
        except Exception:
            logging.error("Неверный формат inactive_period: %s. Используется значение по умолчанию: 7 дней.", s)
            return timedelta(days=7)

    def pluralize(self, number, forms):
        """Возвращает правильную форму слова для числа (например, 1 минута, 2 минуты, 5 минут)."""
        if number % 10 == 1 and number % 100 != 11:
            return forms[0]  # 1 минута, 1 час, 1 день
        elif 2 <= number % 10 <= 4 and (number % 100 < 10 or number % 100 >= 20):
            return forms[1]  # 2–4 минуты, часа, дня
        else:
            return forms[2]  # 0, 5–20 минут, часов, дней

    def to_human_time(self, delta):
        seconds = delta.total_seconds()
        if seconds < 3600:
            minutes = int(seconds // 60)
            return f"{minutes} {self.pluralize(minutes, ['минута', 'минуты', 'минут'])}"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            return f"{hours} {self.pluralize(hours, ['час', 'часа', 'часов'])}"
        else:
            days = int(seconds // 86400)
            return f"{days} {self.pluralize(days, ['день', 'дня', 'дней'])}"

    def escape_markdown(self, text):
        """Экранирование специальных символов для MarkdownV2."""
        if not text:
            return text
        # Экранируем все зарезервированные символы для MarkdownV2
        markdown_chars = r'([*_`\[\]\(\)~#+\-=|{}\.!])'
        return re.sub(markdown_chars, r'\\\1', str(text))

    def send_message(self, message):
        if not self.enabled:
            logging.info("Уведомления Telegram отключены, пропуск отправки сообщения")
            return False
        # Проверяем длину сообщения
        if len(message) > 4096:
            logging.warning("Сообщение слишком длинное (%d символов), урезается до 4096", len(message))
            message = message[:4096]
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": False
        }
        if self.thread_id is not None:
            payload["message_thread_id"] = self.thread_id
        for attempt in range(self.retries):
            try:
                response = requests.post(f"{self.base_url}/sendMessage", json=payload, timeout=10)
                response.raise_for_status()
                logging.info("Сообщение Telegram успешно отправлено в тему %s", self.thread_id or "основная")
                return True
            except requests.exceptions.RequestException as e:
                logging.warning("Попытка отправки Telegram %d не удалась: %s, ответ=%s", 
                                attempt + 1, str(e), getattr(response, 'text', 'Нет ответа'))
                if attempt < self.retries - 1:
                    logging.info("Повторная попытка через %d секунд...", self.retry_interval)
                    time.sleep(self.retry_interval)
            except Exception as e:
                logging.error("Критическая ошибка при отправке сообщения Telegram: %s", str(e))
                break
        logging.error("Не удалось отправить сообщение Telegram после %d попыток", self.retries)
        return False

    def notify(self, mac, ip, hostname, is_new_device, time_diff=None):
        if not self.enabled:
            logging.info("Уведомления Telegram отключены, пропуск уведомления для MAC %s", mac)
            return
        # Экранируем время
        current_time = self.escape_markdown(datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
        # Для неактивных устройств получаем имя из базы данных, если оно не передано
        if not is_new_device and not hostname:
            hostname = self.db_manager.get_hostname(mac)
        # Экранируем пользовательские данные
        hostname = self.escape_markdown(hostname or "Не указано")
        mac = self.escape_markdown(mac)
        ip = self.escape_markdown(ip)
        domain_name = self.escape_markdown(self.domain_name)
        web_url = self.web_url

        # Проверяем корректность URL
        if not web_url.startswith(('http://', 'https://')):
            web_url = f"https://{web_url}"
            logging.warning("URL исправлен на: %s", web_url)

        # Экранируем статические части сообщения
        mac_label = self.escape_markdown("MAC")
        ip_label = self.escape_markdown("Выдан IP")
        
        # Формируем сообщение
        if is_new_device and self.notify_new:
            message = (
                "🛜 Подключено *новое устройство* к сети\n"
                f"ℹ️ *Имя*: {hostname}\n"
                f"🔌 *{mac_label}*: {mac}\n"
                f"✉️ *{ip_label}*: {ip}\n"
                f"🌐 *Сеть*: {domain_name}\n"
                f"📱 Управление: [Открыть]({web_url})\n"
                f"🕒 Время: {current_time}"
            )
            self.send_message(message)
        elif not is_new_device and self.notify_inactive and time_diff:
            human_time = self.escape_markdown(self.to_human_time(time_diff))
            message = (
                "🛜 Устройство подключилось после *длительной неактивности*\n"
                f"🗓 Последнее подключение: *{human_time} назад*\n"
                f"ℹ️ *Имя*: {hostname}\n"
                f"🔌 *{mac_label}*: {mac}\n"
                f"✉️ *{ip_label}*: {ip}\n"
                f"🌐 *Сеть*: {domain_name}\n"
                f"📱 Управление: [Открыть]({web_url})\n"
                f"🕒 Время: {current_time}"
            )
            self.send_message(message)