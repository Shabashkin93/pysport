import threading
import time
import logging
import re
from pywinusb import hid

# Yanpodo VID/PID
YANPODO_VID = 0x1A86
YANPODO_PID = 0xE010


def checksum(data: bytes) -> int:
    """Рассчитывает контрольную сумму для пакета."""
    u_sum = sum(data) & 0xFF  # Суммируем все байты и обрезаем до 8 бит
    checksum = (~u_sum + 1) & 0xFF  # Инверсия + 1, обрезаем до 8 бит
    return checksum


def build_command(addr: int, cmd: int, data: bytes = b"") -> bytes:
    """Создает команду для отправки устройству."""
    length = len(data) + 3
    pkt = bytearray([0x53, 0x57])  # Заголовок
    pkt.extend(length.to_bytes(2, "big"))
    pkt.append(addr)
    pkt.append(cmd)
    pkt.extend(data)
    pkt.append(checksum(pkt))
    return bytes(pkt)


def parse_response(resp: bytes):
    """Парсит ответ от устройства."""
    # Head: 0x43, 0x54
    if resp[:2] != b"\x43\x54":
        raise ValueError("Invalid response header")
    length = resp[3]
    addr = resp[4]
    cmd = resp[5]
    status = resp[6]
    data = resp[7:-1]
    check = resp[-1]
    if checksum(resp[:-1]) != check:
        raise ValueError("Checksum error")
    return {"addr": addr, "cmd": cmd, "status": status, "data": data}


def list_hid_readers():
    devices = []
    for dev in hid.HidDeviceFilter(
        vendor_id=YANPODO_VID, product_id=YANPODO_PID
    ).get_devices():
        # Пропускаем интерфейсы с KBD в path (эмуляция клавиатуры)
        if "KBD" in dev.device_path or "kbd" in dev.device_path:
            continue
        devices.append(dev)
    return devices


class RFIDReaderThread(threading.Thread):
    def __init__(self, device_info, callback=None, logger=None):
        super().__init__(daemon=True)
        self.device_info = device_info  # pywinusb device object
        self.callback = callback
        self._stop_event = threading.Event()
        self.dev = None
        self.logger = logger or logging.getLogger("RFIDReaderThread")
        self.report_id = None

    def open(self):
        self.dev = self.device_info
        self.dev.open()
        # Найти нужный Report ID (обычно 0x07, но не хардкодим)
        # out_reports = self.dev.find_output_reports()
        # # # Выбираем report с report_id == 0x07
        # for rep in out_reports:
        #     if rep.report_id == 0x07:
        #         self.report = rep
        #         self.report_id = rep.report_id
        #         break
        # else:
        #     raise RuntimeError("No output report with Report ID 0x07 found")

    def close(self):
        if self.dev:
            self.dev.close()
            self.dev = None

    def send_command(self, cmd, data=b"", timeout=1.0):
        """
        Отправляет команду устройству и ожидает ответа.
        :param cmd: Код команды.
        :param data: Данные команды.
        :param timeout: Таймаут ожидания ответа (в секундах).
        :return: Ответ от устройства.
        """
        if not self.dev:
            raise RuntimeError("Устройство не подключено.")

        try:
            # Формируем пакет команды
            addr = 0xFF
            pkt = build_command(addr, cmd, data)
            report_id = 0x07
            buf = [report_id] + list(pkt)
            buf.extend([0x00] * (64 - len(buf)))  # Дополняем до 64 байт

            # Отправляем данные
            self.dev.write(buf)
            self.logger.info(f"Отправлено: {buf}")

            # Ожидаем ответ
            start_time = time.time()
            while time.time() - start_time < timeout:
                response = self.dev.read(64)
                if response:
                    self.logger.info(f"Получено: {response}")
                    return parse_response(bytes(response[1:]))
                time.sleep(0.1)

            raise TimeoutError("Ответ от устройства не получен.")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке команды: {e}")
            raise

    def clear_buffer(self):
        """Очищает буфер входящих данных."""
        if not self.dev:
            raise RuntimeError("Устройство не подключено.")

        try:
            for _ in range(10):
                response = self.dev.read(64)
                if response:
                    self.logger.debug(f"Очищено: {response}")
                time.sleep(0.01)
        except Exception as e:
            self.logger.error(f"Ошибка очистки буфера: {e}")

    def run(self):
        try:
            self.open()
            # Логирование serial (если найден)
            path = self.dev.device_path
            match = re.search(r"#b&([0-9a-fA-F]+)&0&", path)
            if match:
                serial = match.group(1)
                self.logger.info(f"Opened device serial: {serial}")

            # 1. Очистка буфера (на всякий случай)
            self.clear_buffer()
            self.logger.info("Cleared device buffer")

            # 2. Чтение параметров системы (опционально)
            try:
                sysinfo = self.send_command(0x10)
                self.logger.info(f"System param: {sysinfo}")
            except Exception as e:
                self.logger.warning(f"Could not read system param: {e}")
            # 3. Чтение параметров устройства (опционально)
            try:
                devinfo = self.send_command(0x20)
                self.logger.info(f"Device param: {devinfo}")
            except Exception as e:
                self.logger.warning(f"Could not read device param: {e}")

            # 4. Основной цикл – обработка входящих сообщений без опроса
            self.logger.info("Start listening for tag data (CMD 0x45)...")

            def on_input_report(rep):
                try:
                    data = rep.get()
                    payload = bytes(data[1:])
                    report_id = data[0]
                    # Если пакет с тегом (ответ начинается с 0x43 0x54)
                    if payload.startswith(b"\x43\x54"):
                        self.logger.info(f"Tag packet detected: {payload.hex()}")
                        try:
                            parsed = parse_response(payload)
                            self.logger.debug(f"Parsed response: {parsed}")
                            # Если пакет содержит команду 0x45 с успешным статусом
                            if parsed["cmd"] == 0x45 and parsed["status"] == 1:
                                if self.callback:
                                    self.callback(
                                        parsed, {"path": self.dev.device_path}
                                    )
                        except Exception as e:
                            self.logger.error(f"Parse error: {e}")
                except Exception as e:
                    self.logger.error(f"Error reading input report: {e}")

            # Регистрируем callback на входящие данные – устройство само передаёт их (URB_INTERRUPT IN)
            self.dev.set_raw_data_handler(on_input_report)

            while not self._stop_event.is_set():
                time.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Thread run error: {e}", exc_info=True)
        finally:
            self.close()

    def stop(self):
        self._stop_event.set()


# Пример интеграции:
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def tag_callback(parsed, devinfo):
        print(f"Tag from {devinfo.get('serial_number', devinfo['path'])}: {parsed}")

    readers = list_hid_readers()
    threads = []
    for dev in readers:
        t = RFIDReaderThread(dev, callback=tag_callback)
        t.start()
        threads.append(t)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for t in threads:
            t.stop()
        for t in threads:
            t.join()
    # for rep in dev.find_output_reports():
    #     print(f"Output report: id={rep.report_id}, len={len(rep.get())}")
