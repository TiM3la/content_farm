import os
import time
from huggingface_hub import InferenceClient

# --- НАСТРОЙКИ ---
HF_TOKEN = "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"
OUTPUT_DIR = "quotes"

# СПИСОК МОДЕЛЕЙ (от мощных к более легким)
MODELS_POOL = [
    "Qwen/Qwen2.5-72B-Instruct",
    "Qwen/Qwen2.5-7B-Instruct",
    "mistralai/Mistral-7B-Instruct-v0.3",
    "meta-llama/Llama-3.2-3B-Instruct",
    "google/gemma-2-9b-it"
]

# Глобальная переменная для текущей модели
current_model_index = 0

client = InferenceClient(api_key=HF_TOKEN)


def get_current_model():
    return MODELS_POOL[current_model_index]


def switch_model():
    """Переключает на следующую модель в списке"""
    global current_model_index
    if current_model_index < len(MODELS_POOL) - 1:
        current_model_index += 1
        print(f"🔄 Переключаюсь на резервную модель: {get_current_model()}")
        return True
    else:
        print("❌ Все модели в списке исчерпали лимиты или недоступны.")
        return False


def call_ai(prompt, temp=0.7, tokens=1500):
    """Запрос к AI с логикой переключения при ошибках"""
    while True:
        model_id = get_current_model()
        try:
            response = client.chat_completion(
                model=model_id,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=tokens,
                temperature=temp
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            error_msg = str(e)
            print(f"⚠️ Ошибка на модели {model_id}: {error_msg[:100]}...")

            # Если ошибка 402 (лимиты) или модель не найдена/перегружена (503, 404)
            if "402" in error_msg or "429" in error_msg or "503" in error_msg:
                if not switch_model():
                    return None
                time.sleep(2)  # Пауза перед сменой модели
                continue
            else:
                return None


def verify_quote(quote_text):
    verify_prompt = f"""Проверь текст на роль глубокой цитаты для Shorts. 
    Критерии: философская мысль, отсутствие имен героев, законченный смысл.
    Текст: "{quote_text}"
    Ответь СТРОГО одним словом: ПОДХОДИТ или НЕТ."""

    result = call_ai(verify_prompt, temp=0.1, tokens=10)
    return result and "ПОДХОДИТ" in result.upper()


def process_book(file_path, chunk_size=5000):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            book_text = f.read()
    except FileNotFoundError:
        print(f"❌ Файл {file_path} не найден.")
        return

    chunks = [book_text[i:i + chunk_size] for i in range(0, len(book_text), chunk_size)]
    quote_counter = 1

    print(f"🚀 Начинаю анализ книги. Текущая модель: {get_current_model()}\n")

    for i, chunk in enumerate(chunks):
        print(f"🔍 Часть {i + 1}/{len(chunks)}...")

        search_prompt = f"""Ты — редактор. Найди в тексте философские мысли (40-120 слов). 
        Без диалогов. Разделяй фрагменты знаком ***. НЕ пиши вступлений.
        Текст: {chunk}"""

        raw_content = call_ai(search_prompt)

        if raw_content:
            candidates = [q.strip() for q in raw_content.split('***') if len(q.strip()) > 20]
            for candidate in candidates:
                if verify_quote(candidate):
                    file_name = f"quote_{quote_counter}.txt"
                    with open(os.path.join(OUTPUT_DIR, file_name), 'w', encoding='utf-8') as q_file:
                        q_file.write(candidate)
                    print(f"   ✅ Сохранено в {file_name}")
                    quote_counter += 1
                    time.sleep(1)

        time.sleep(2)

    print(f"\n✨ Работа завершена! Сохранено цитат: {quote_counter - 1}")


# Запуск
process_book("book_2.txt")