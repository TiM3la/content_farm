import os
import time
from huggingface_hub import InferenceClient

# 1. Настройка
HF_TOKEN = "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"
MODEL_ID = "Qwen/Qwen2.5-72B-Instruct"

client = InferenceClient(api_key=HF_TOKEN)


def extract_philosophical_parts(text_chunk):
    prompt = f"""Ты — литературный редактор. Найди в тексте книги глубокие философские фрагменты для видео (Shorts).

    Критерии:
    - Тема: смысл жизни, мудрость, природа человека.
    - Без диалогов, только законченные мысли.
    - Длина каждого: 40-120 слов.
    - ЕСЛИ фрагментов несколько, разделяй их строго знаком ***.
    - НЕ пиши никаких вступлений. ТОЛЬКО найденный текст.

    Текст для анализа:
    {text_chunk}
    """

    try:
        response = client.chat_completion(
            model=MODEL_ID,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
            temperature=0.7
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Ошибка API: {e}"


def process_book(file_path, chunk_size=5000):
    # Создаем папку, если ее нет
    output_dir = "quotes"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Создана папка: {output_dir}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            book_text = f.read()
    except FileNotFoundError:
        print(f"❌ Ошибка: Файл {file_path} не найден.")
        return

    chunks = [book_text[i:i + chunk_size] for i in range(0, len(book_text), chunk_size)]

    quote_counter = 1  # Счётчик для имен файлов
    print(f"--- Обработка книги: {len(chunks)} частей ---\n")

    for i, chunk in enumerate(chunks):
        print(f'⏳ Часть {i + 1}/{len(chunks)}...')

        content = extract_philosophical_parts(chunk)

        if content and "Ошибка API:" not in content:
            # Разделяем ответ на отдельные цитаты по знаку ***
            individual_quotes = [q.strip() for q in content.split('***') if len(q.strip()) > 10]

            if individual_quotes:
                print(f'✅ Найдено изречений: {len(individual_quotes)}')

                for quote in individual_quotes:
                    file_name = f"quote_{quote_counter}.txt"
                    file_path = os.path.join(output_dir, file_name)

                    # Записываем в файл
                    with open(file_path, 'w', encoding='utf-8') as q_file:
                        q_file.write(quote)

                    print(f"   💾 Сохранено в {file_name}")
                    quote_counter += 1
            else:
                print(f'🔘 В части {i + 1} ничего подходящего.')
        else:
            print(f'⚠️ Ошибка или пустой ответ в части {i + 1}.')

        time.sleep(2)

    print(f"\n✨ Готово! Все файлы сохранены в папку '{output_dir}'.")


# Запуск
process_book("my_book.txt")