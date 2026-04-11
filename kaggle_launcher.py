import yadisk as yd, time, os
from kaggle.api.kaggle_api_extended import KaggleApi
from gradio_client import Client
from dotenv import load_dotenv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

if os.path.exists(BASE_DIR / '.env'):
    load_dotenv(BASE_DIR / '.env')

yd_token = os.getenv('YANDEX_DISK_API_TOKEN')
y = yd.YaDisk(token=yd_token)

def get_remote_gradio_url(y_client, url_file_name):
    """Скачивает свежую ссылку с Яндекс.Диска"""
    remote_path = f'app:/gradio_urls/{url_file_name}'
    local_path = BASE_DIR / 'temp' / 'gradio_urls' / f'{url_file_name}'

    print("[Процесс...] Ищем актуальную ссылку на Яндекс.Диске...")
    try:
        if y_client.exists(remote_path):
            y_client.download(remote_path, str(local_path))
            with open(local_path, "r") as f:
                url = f.read().strip()
            os.remove(local_path)
            print(f'[ОК] Ссылка на dragio получена: {url}')
            return url
        else:
            print("[!] Файл со ссылкой еще не создан в облаке.")
            time.sleep(20)
            return None
    except Exception as e:
        print(f"[!] Ошибка получения ссылки: {e}")
        return None

def run_kaggle_notebook(url_file_name, notebook, json_file_path):
    # Чистим старую ссылку
    try:
        y.remove(f'app:/gradio_urls/{url_file_name}')
    except:
        pass

    api = KaggleApi()
    api.authenticate()

    # запускаем ноутбук
    print(f"[Процессс..] Запускаем ноутбук {notebook}...")
    try:
        api.kernels_push(json_file_path)
        print("[ОК] Ноутбук запускается")
    except Exception as e:
        print(f"[!] Ошибка при запуске ноутбука {e}")
        return

    # ждем начало сессии
    print("[Процесс...] Ждем начало работы сессии ноутбука:")
    for _ in range(40):
        try:
            status_info = api.kernels_status(notebook)
            status = getattr(status_info, 'status', 'UNKNOWN')
            print(f"[{time.strftime('%H:%M:%S')}] Статус: {status}")
            if 'running' in str(status).lower() or 'complete' in str(status).lower():
                print("[ОК] Сессия ноутбука стартовала")
                break

        except Exception as e:
            if "403" in str(e):
                print(
                    f"[!] 403 Forbidden. Проверь: 1. Совпадает ли notebook в коде и на сайте. 2. Принят ли Phone Verification на Kaggle.")
            else:
                print(f"[...] Ожидание инициализации: {e}")

        time.sleep(15)

    # 1. Пытаемся получить ссылку
    gradio_url = None
    while gradio_url is None:
        print('[Процесс...] Ищем ссылку на gradio')
        gradio_url = get_remote_gradio_url(y, url_file_name)

    if gradio_url:
        print(f"[ПК] Подключаемся к {gradio_url}...")
        client = None

        for attempt in range(5):
            try:
                client = Client(gradio_url)
                print(f"[ПК] Соединение установлено! Отправляем данные...")
                break

            except Exception as e:
                print(f"[!] Попытка {attempt + 1}: Сервер еще не готов ({e})")
                if attempt < 4:
                    time.sleep(10)
                else:
                    print("[!!!] Критическая ошибка связи.")

        return client

