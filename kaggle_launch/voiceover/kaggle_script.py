import sys
import os
import subprocess
import time


# Функция для мгновенного вывода в консоль Kaggle
def log(msg):
    print(f"[DEBUG] {msg}", flush=True)
    sys.stdout.flush()


log("Step 1: Start script")

# 1. Быстрая установка
try:
    import gradio as gr
    import yadisk

    log("Libraries already installed")
except ImportError:
    log("Installing dependencies...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "gradio", "yadisk"])
    import gradio as gr
    import yadisk

# 2. Твой токен (ВСТАВЬ ЕГО СЮДА ТЕКСТОМ)
TOKEN = "y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg"

# 3. Запуск сервера
try:
    log("Step 2: Launching Gradio")


    def tts_fn(t):
        return f"Ready: {t}"


    demo = gr.Interface(fn=tts_fn, inputs="text", outputs="text")

    # Запускаем и сразу берем ссылку
    demo.launch(share=True, inline=False, prevent_thread_lock=True)
    url = demo.share_url
    log(f"Step 3: URL IS {url}")

    with open("gradio_url.txt", "w") as f:
        f.write(url)
except Exception as e:
    log(f"Gradio Error: {e}")

# 4. Загрузка на Яндекс.Диск
log("Step 4: Connecting to Yandex...")
try:
    y = yadisk.YaDisk(token=TOKEN)
    remote_path = "app:/gradio_url.txt"

    if y.exists(remote_path):
        log("Removing old file on Disk...")
        y.remove(remote_path)

    y.upload("gradio_url.txt", remote_path)
    log("Step 5: UPLOAD SUCCESSFUL!")
except Exception as e:
    log(f"Yandex Error: {e}")

# 5. Бесконечный цикл с отчетом каждые 30 сек
log("Step 6: Entering main loop...")
while True:
    log("Still alive and waiting...")
    time.sleep(30)