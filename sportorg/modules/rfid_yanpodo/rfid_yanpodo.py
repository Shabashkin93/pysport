import datetime
import time
import logging
from queue import Queue, Empty
from threading import Event, main_thread
from ctypes import byref, c_int
import threading
import hid

from random import randint
from fastapi import FastAPI, Request
from pydantic import BaseModel
import uvicorn

from PySide6.QtCore import QThread, Signal
from sportorg.common.otime import OTime
from sportorg.common.singleton import singleton
from sportorg.models import memory
from sportorg.models.memory import race

# Yanpodo VID/PID
YANPODO_VID = 0x1A86
YANPODO_PID = 0xE010


class YanpodoCommand:
    def __init__(self, command, data=None):
        self.command = command
        self.data = data


class YanpodoThread(QThread):
    def __init__(self, interface, port, queue, stop_event, logger, debug=False):
        super().__init__()
        self.setObjectName(self.__class__.__name__)
        self.interface = interface  # "USB", "COM", or "TCP"
        self.device = None
        self.serial_number = ''  # Serial number of the device
        self.port = port
        self._queue = queue
        self._stop_event = stop_event
        self._logger = logger
        self._debug = debug
        self.timeout_list = {}
        self.timeout = race().get_setting("readout_duplicate_timeout", 15000)
        self._fastapi_app = None
        self._uvicorn_server = None

    def stop_timers(self):
        if hasattr(self, "_epc_timers"):
            for timer in self._epc_timers.values():
                timer.cancel()
            self._epc_timers.clear()

    def run(self):
        try:
            if self.interface == "USB":
                self._initialize_usb()
            elif self.interface == "COM":
                self._initialize_com()
            elif self.interface == "TCP":
                self._run_tcp_server()
                return  # TCP server loop is blocking

            self._logger.info(
                f"Yanpodo {self.serial_number} initialized successfully on {self.interface} interface."
            )

            while not self._stop_event.is_set():
                self._read_tags()

        except Exception as e:
            self._logger.error(
                f"Error in YanpodoThread {self.serial_number}: {e}"
            )
        finally:
            self.stop_timers()
            self._logger.info("Stopping YanpodoThread")

    def _initialize_usb(self):
        self.device = hid.device()
        self.device.open_path(self.port)
        self._logger.debug(f"Connected to device: {self.device.get_product_string()}")

        self.device.set_nonblocking(True)

        CMD_READ_SYSTEM_PARAM = [0x00, 0x07, 0x53, 0x57, 0x00, 0x03, 0xFF, 0x10, 0x44]

        # Отправка данных (через interrupt OUT transfer за кулисами)
        bytes_written = self.device.write(CMD_READ_SYSTEM_PARAM)

        # Ждем и читаем ответ (через interrupt IN transfer за кулисами)
        for _ in range(10):
            response = self.device.read(64)  # Максимальная длина пакета
            if response:
                for i in range(10, 17):
                    self.serial_number += hex(response[i]).replace("0x", "").zfill(2)
                self.serial_number = self.serial_number.upper()
                self._logger.debug(f"DevSN: {self.serial_number}")
                break
            time.sleep(0.1)

    def _initialize_com(self):
        if self.reader_lib.SWCom_OpenDevice(self.port, 115200) != 1:
            raise RuntimeError(f"Failed to open COM device on port {self.port}")
        self.reader_lib.SWCom_ClearTagBuf()

    def _run_tcp_server(self):
        # FastAPI app for TCP server
        app = FastAPI()

        class TcpCardData(BaseModel):
            epc: str
            finish_time: str
            deviceId: str

        @app.post("/rfid")
        async def receive_tag(data: TcpCardData):
            arr_buffer = bytes.fromhex(data.epc)
            self._process_tags(
                arr_buffer, tag_count=1, time=data.finish_time, deviceSN=data.deviceId
            )
            return {"status": "ok"}

        uvicorn_log_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "custom": {
                    "format": "%(levelname)s %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S"
                }
            },
            "handlers": {
                "default": {
                    "formatter": "custom",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                }
            },
            "loggers": {
                "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
                "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
                "uvicorn.access": {
                    "handlers": ["default"],
                    "level": "WARNING",
                    "propagate": False,
                },
            }
        }
        
        config = uvicorn.Config(
            app, host="0.0.0.0", port=self.port, log_level="info", loop="asyncio",
            http="h11",
            log_config=uvicorn_log_config
        )
        self._uvicorn_server = uvicorn.Server(config)
        self._logger.info(f"TCP FastAPI server started on port {self.port}")
        import asyncio

        async def shutdown_trigger():
            while not self._stop_event.is_set():
                await asyncio.sleep(0.2)
            await self._uvicorn_server.shutdown()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self._uvicorn_server.serve())
        loop.run_until_complete(shutdown_trigger())

    def _read_tags(self):
        arr_buffer = bytes(9182)
        tag_length = c_int(0)
        tag_number = c_int(0)

        if self.interface == "USB":
            for _ in range(10):
                response = self.device.read(64)  # Максимальная длина пакета
                if response:
                    self._logger.debug(
                        f"Received: {' '.join(f'{b:02X}' for b in response)}"
                    )
                    arr_buffer = bytes(response)
                    tag_number.value = 1
                    break
                time.sleep(0.1)

        elif self.interface == "COM":
            ret = self.reader_lib.SWCom_GetTagBuf(
                arr_buffer, byref(tag_length), byref(tag_number)
            )
        else:
            return

        if tag_number.value > 0:
            self._process_tags(arr_buffer, tag_number.value)

    def _process_tags(self, arr_buffer, tag_count, time=None, deviceSN=None):

        if deviceSN is not None:
            deviceSN = str(deviceSN).upper() # для MAC-адресов полученных TCP-сервером
        else:
            deviceSN = str(self.serial_number).upper()

        # Для хранения последних card_data и таймеров по epc
        if not hasattr(self, "_epc_timers"):
            self._epc_timers = {}
            self._epc_data = {}

        # Если получаем RAW UBR пакет, то в обработку идут только байты с 16 по 31
        if len(arr_buffer) > 15 and arr_buffer[0] == 0x20:
            tag_buffer = arr_buffer[16:31]
        elif len(arr_buffer) == 15 and arr_buffer[0] == 0x0F:
            tag_buffer = arr_buffer
        else:
            self._logger.warning(
                f"[{self.serial_number}] Unknown tag buffer: len={len(arr_buffer)}, first_byte={arr_buffer[0] if arr_buffer else 'N/A'}"
            )
            return  # Просто выходим, не обрабатываем

        i_length = 0
        for _ in range(tag_count):
            b_pack_length = tag_buffer[i_length]
            self._logger.debug(
                f"b_pack_length: {b_pack_length}, i_length: {i_length}, tag_buffer: {' '.join(f'{b:02X}' for b in tag_buffer)}"
            )

            epc = ""
            for i in range(2, b_pack_length - 1):
                epc += hex(tag_buffer[1 + i_length + i]).replace("0x", "").zfill(2)

            if time is None:
                time = OTime.now()
            elif isinstance(time, str):
                try:
                    # Преобразуем ISO-строку в datetime
                    dt = datetime.datetime.fromisoformat(time)
                    # Создаём OTime на основе времени
                    time = OTime(
                        day=0,
                        hour=dt.hour,
                        minute=dt.minute,
                        sec=dt.second,
                        msec=dt.microsecond // 1000
                    )
                except Exception as e:
                    self._logger.error(f"Invalid time format: {time} ({e})")
                    time = OTime.now()

            card_data = {"epc": epc, "time": time}
            self._logger.info(
                f"[{deviceSN}] Tag read: {card_data}"
            )

            # Просто отправляем card_data в очередь без проверки дубликатов
            try:
                self._queue.put(YanpodoCommand("card_data", card_data), timeout=1)
            except Exception as e:
                self._logger.error(f"Failed to put card_data: {e}")

            i_length += b_pack_length + 1


class ResultThread(QThread):
    data_sender = Signal(object)

    def __init__(self, queue, stop_event, logger):
        super().__init__()
        self.setObjectName(self.__class__.__name__)
        self._queue = queue
        self._stop_event = stop_event
        self._logger = logger
        self._epc_data = {}  # card_id -> card_data
        self._epc_timers = {}  # card_id -> timer

    def run(self):
        while not self._stop_event.is_set():
            try:
                cmd = self._queue.get(timeout=5)
                if cmd.command == "card_data":
                    card_data = cmd.data
                    card_id = card_data["epc"]
                    card_time = card_data["time"]
                    timeout = (
                        race().get_setting("readout_duplicate_timeout", 5000) / 1000
                    )

                    # Сохраняем только результат с максимальным временем
                    prev_data = self._epc_data.get(card_id)
                    if prev_data is None or card_time > prev_data["time"]:
                        self._epc_data[card_id] = card_data

                    # Если таймер уже есть, сбрасываем его
                    if card_id in self._epc_timers:
                        self._epc_timers[card_id].cancel()

                    # Запускаем новый таймер на timeout
                    def send_result(epc=card_id):
                        data = self._epc_data.pop(epc, None)
                        self._epc_timers.pop(epc, None)
                        if data:
                            try:
                                result = self._get_result(data)
                                self.data_sender.emit(result)
                            except Exception as e:
                                self._logger.error(f"Failed to emit result: {e}")

                    timer = threading.Timer(timeout, send_result)
                    self._epc_timers[card_id] = timer
                    timer.start()

            except Empty:
                if not main_thread().is_alive() or self._stop_event.is_set():
                    break
            except Exception as e:
                self._logger.exception(e)
        self._logger.debug("Stopping ResultThread")

    @staticmethod
    def _get_result(card_data):
        result = memory.race().new_result(memory.ResultRfidYanpodo)

        limit = 10**8
        hex_offset = 5000000
        epc = str(card_data["epc"]).replace(" ", "")

        # Если EPC содержит только цифры, используем их напрямую
        # Иначе конвертируем hex -> dec и добавляем offset
        if epc.isdecimal():
            result.card_number = int(epc) % limit
        else:
            result.card_number = (int(epc, 16) + hex_offset) % limit

        result.finish_time = card_data["time"]
        return result


@singleton
class YanpodoClient:
    def __init__(self):
        self._queue = Queue()
        self._stop_event = Event()
        self._yanpodo_threads = {}  # path -> thread
        self._result_thread = None
        self._logger = logging.getLogger("YanpodoClient")
        self._logger.setLevel(logging.INFO)
        self._call_back = None

    def set_call(self, value):
        if self._call_back is None:
            self._call_back = value
        return self

    def _start_yanpodo_thread(self, interface):
        if self._yanpodo_thread is None:
            self._yanpodo_thread = YanpodoThread(
                interface, self.port, self._queue, self._stop_event, self._logger
            )
            self._yanpodo_thread.start()
        elif self._yanpodo_thread.isFinished():
            self._yanpodo_thread = None
            self._start_yanpodo_thread(interface)

    def _start_result_thread(self):
        if self._result_thread is None:
            self._result_thread = ResultThread(
                self._queue,
                self._stop_event,
                self._logger,
            )
            if self._call_back is not None:
                self._result_thread.data_sender.connect(self._call_back)
            self._result_thread.start()
        elif self._result_thread.isFinished():
            self._result_thread = None
            self._start_result_thread()

    def is_alive(self):
        # Считаем, что клиент жив, если есть хотя бы один активный поток
        any_thread_alive = any(
            thread is not None and not thread.isFinished()
            for thread in self._yanpodo_threads.values()
        )
        return (
            any_thread_alive
            and self._result_thread is not None
            and not self._result_thread.isFinished()
        )

    def _monitor_thread(self, interface, path):
        """Следит за потоком и перезапускает его при сбое, если не поступила команда на остановку.
        Сбрасывает счетчик i, если поток проработал более 60 секунд."""
        i = 0
        while not self._stop_event.is_set():
            start_time = time.time()
            thread = YanpodoThread(
                interface, path, self._queue, self._stop_event, self._logger
            )
            self._yanpodo_threads[path] = thread
            thread.start()
            thread.wait()  # Ждём завершения потока
            elapsed = time.time() - start_time
            if elapsed > 60:
                i = 0  # Сбросить счетчик, если поток проработал больше 60 секунд
            if not self._stop_event.is_set():
                i += 1
                if i > 5:
                    self._logger.error(
                        f"YanpodoThread on {path} has crashed too many times, stopping monitoring."
                    )
                    break
                self._logger.warning(f"YanpodoThread on {path} crashed, restarting...")
                time.sleep(3)  # Короткая пауза перед перезапуском

    def start(self, interface="USB"):
        self._stop_event.clear()
        paths = []
        for dev in hid.enumerate():
            if dev["vendor_id"] == YANPODO_VID and dev["product_id"] == YANPODO_PID:
                # Пропускаем интерфейсы с KBD в path (эмуляция клавиатуры)
                path_str = (
                    dev["path"].decode()
                    if isinstance(dev["path"], bytes)
                    else dev["path"]
                )
                if "KBD" in path_str or "kbd" in path_str:
                    continue
                paths.append(dev["path"])
                self._logger.debug(f"Found device: {dev['path']}")
        for path in paths:
            if (
                path not in self._yanpodo_threads
                or self._yanpodo_threads[path] is None
                or self._yanpodo_threads[path].isFinished()
            ):
                t = threading.Thread(
                    target=self._monitor_thread, args=(interface, path), daemon=True
                )
                t.start()

        # Всегда запускаем TCP сервер на отдельном порту (например, 9000)
        tcp_port = 8000
        if (
            "TCP" not in self._yanpodo_threads
            or self._yanpodo_threads["TCP"] is None
            or self._yanpodo_threads["TCP"].isFinished()
        ):
            tcp_thread = YanpodoThread(
                "TCP", tcp_port, self._queue, self._stop_event, self._logger
            )
            tcp_thread.start()
            self._yanpodo_threads["TCP"] = tcp_thread

        self._start_result_thread()

    def toggle(self, interface="USB"):
        if self.is_alive():
            self.stop()
        else:
            self.start(interface)

    def choose_port(self):
        return memory.race().get_setting("system_port", "COM4")

    def stop(self):
        self._stop_event.set()
        time.sleep(0.2)
        for thread_key, thread in list(self._yanpodo_threads.items()):
            if thread:
                try:
                    # Wait with timeout to avoid blocking indefinitely
                    if thread.isRunning():
                        finished = thread.wait(3000)
                        self._logger.debug(
                            f"Thread {threading.current_thread().name} finished: {finished}"
                        )

                    # If thread didn't finish, try to terminate it more forcefully
                    if not finished and thread.isRunning():
                        self._logger.warning(
                            f"Thread {threading.current_thread().name} did not finish gracefully, terminating"
                        )
                        thread.terminate()
                        thread.wait(1000)  # Give it another second
                except Exception as e:
                    self._logger.error(f"Error stopping thread {thread_key}: {e}")
                # Remove the thread reference
                self._yanpodo_threads[thread_key] = None
        # Clear the thread dictionary
        self._yanpodo_threads.clear()

        # Handle the result thread similarly
        if self._result_thread and self._result_thread.isRunning():
            try:
                finished = self._result_thread.wait(3000)
                self._logger.debug(f"Result thread finished: {finished}")

                if not finished and self._result_thread.isRunning():
                    self._logger.warning(
                        "Result thread did not finish gracefully, terminating"
                    )
                    self._result_thread.terminate()
                    self._result_thread.wait(1000)
            except Exception as e:
                self._logger.error(f"Error stopping result thread: {e}")

            self._result_thread = None

        self._logger.info("YanpodoClient stopped")


if __name__ == "__main__":
    client = YanpodoClient()
    try:
        client.start(interface="USB")
    except KeyboardInterrupt:
        client.stop()
