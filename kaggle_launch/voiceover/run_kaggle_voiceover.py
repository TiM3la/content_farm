# run_kaggle_voiceover.py
import time, sys, io
import os


from huggingface_hub import InferenceClient
from kaggle.api.kaggle_api_extended import KaggleApi
from gradio_client import Client
import yadisk as yd
from dotenv import load_dotenv


yd_token = 'y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg'
os.environ["PYTHONUTF8"] = "1"
SLUG = "tim3la/voiceover-1"
FOLDER = "C:/python_projects_tim3la/content_farm/kaggle_launch/voiceover/"

y = yd.YaDisk(token=yd_token)

def get_remote_gradio_url(y_client):
    """Скачивает свежую ссылку с Яндекс.Диска"""
    remote_path = "app:/gradio_url.txt"
    local_path = "temp_gradio_url.txt"

    print("[Процесс...] Ищем актуальную ссылку на Яндекс.Диске...")
    try:
        if y_client.exists(remote_path):
            y_client.download(remote_path, local_path)
            with open(local_path, "r") as f:
                url = f.read().strip()
            os.remove(local_path)
            return url
        else:
            print("[!] Файл со ссылкой еще не создан в облаке.")
            return None
    except Exception as e:
        print(f"[!] Ошибка получения ссылки: {e}")
        return None

def run_kaggle_voiceover():
    try:
        y.remove('app:/gradio_url.txt')  # Чистим старую ссылку
    except:
        pass

    api = KaggleApi()
    api.authenticate()

    print(f"[>] Пушим код в {SLUG}...")
    try:
        api.kernels_push(FOLDER)
        print("[ОК] Push прошел.")
    except Exception as e:
        print(f"[!] Ошибка при Push: {e}")
        return

    print("[>] Мониторинг (ждем появления сессии):")

    for _ in range(40):
        try:
            status_info = api.kernels_status(SLUG)
            # В некоторых версиях API статус лежит в .status, в других в ['status']
            # Попробуем самый безопасный вариант:
            status = getattr(status_info, 'status', 'UNKNOWN')

            print(f"[{time.strftime('%H:%M:%S')}] Статус: {status}")

            if 'running' in str(status).lower() or 'complete' in str(status).lower():
                print("\n[ОК] Работает!")
                break

        except Exception as e:
            if "403" in str(e):
                print(
                    f"[!] 403 Forbidden. Проверь: 1. Совпадает ли SLUG в коде и на сайте. 2. Принят ли Phone Verification на Kaggle.")
            else:
                print(f"[...] Ожидание инициализации: {e}")

        time.sleep(15)

    # 1. Пытаемся получить ссылку
    gradio_url = None
    while gradio_url is None:
        print('[Процесс...] Ищем ссылку на gradio')
        gradio_url = get_remote_gradio_url(y)
        print(gradio_url)
        time.sleep(10)

    if gradio_url:
        print(f"[ПК] Подключаемся к {gradio_url}...")
        client = None

        for attempt in range(5):  # Пробуем 3 раза
            try:
                client = Client(gradio_url)
                print(f"[ПК] Соединение установлено! Отправляем данные...")

                break  # Если успешно, выходим из цикла попыток

            except Exception as e:
                print(f"[!] Попытка {attempt + 1}: Сервер еще не готов ({e})")
                if attempt < 4:
                    time.sleep(10)  # Даем облаку больше времени на "прогрев"
                else:
                    print("[!!!] Критическая ошибка связи.")

        return client