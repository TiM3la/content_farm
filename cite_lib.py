# cite_lib.py

import time
import duckdb as d
import yadisk as yd
import numpy as np
import pandas as pd
import os, mmap, codecs
from dotenv import load_dotenv
from huggingface_hub import InferenceClient
import subprocess, re
from gradio_client import Client
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle_launcher import run_kaggle_notebook
from pathlib import Path
from groq import Groq


BASE_DIR = Path(__file__).resolve().parent

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

yd_token = os.getenv('YANDEX_DISK_API_TOKEN')
higging_face_token = os.getenv('HUGGING_FACE_API_TOKEN')
groq_token = os.getenv('GROQ_TOKEN')
db_name = 'main_db.duckdb'
# параметры фрагментирования
fragment_size = 4000
fragment_overlap = 300
# Анализ текста
model_id = "Qwen/Qwen2.5-7B-Instruct"
model_groq = 'llama-3.1-8b-instant'
system_prompt_generate = (
    "Ты — эксперт, который вычленяет из текстов интересные высказывания "
    "Твоя задача: найти в тексте интересные высказывания, философские изречения или жизненные наблюдения. "
    "ИНСТРУКЦИЯ:\n"
    "1. Выпиши цитаты(они могут состоять из нескольких предложений), которые могут звучать интересно, философски без контекста, нести какую то отдельную, законченную мысль\n"
    "2. Эти цитаты должны быть в объеме до 200 слов.\n"
    "3. В ответе пиши только сами цитаты, если их несколько во фрагменте, разделяй их знаком ***\n"
    "4. Не выбирай предложения, которые комбинируют несколько языков. Только один, на котором книга\n"
    "5. Предложения должны быть законченными, нельзя обрывать слова или оставлять предложения незаконченными\n"
    "6. Все предложения одной цитаты должны быть взаимосвязаны по смыслу\n"
)
system_prompt_check = (
    "Проверь этот текст по следующим параметрам:"
    "Текст представляет собой законченную мысль, он не обрывается на полуслове. Объем текста - до 200 слов. Текст состоит только из русских слов. Текст содержит в себе интересную мысль, философское изречение или жизненное наблюдение, или рассуждение. Текст понятен без контекста и не оставляет вопросов: о ком или о чем идет речь? В нем не упоминаются непонятные имена."
    "Если текст соответствует этим требованиям, отредактируй его: убери лишние слова в скобках, какие-то лишние пометки и комментарии, которые бы диктор не стал читать. В результате выведи только этот отредактированный текст. Только сам текст, без твоих комментариев!"
    "Если текст не соответствует требованиям, выведи только слово NONE"
)
# озвучка
voiceover_types = {
    1: 'qenat_voice_2.wav'
}

class Main_DB:
    def __init__(self, db_name, yd_token):
        self.base = d.connect(BASE_DIR / db_name)
        self.y = yd.YaDisk(token=yd_token)

        print(self.make_book_table())
        print(self.make_fragment_table())
        print(self.make_quote_table())
        print(self.make_voiceove_table())

    def make_book_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Book')
            self.base.execute('create table if not exists Book (id integer primary key, title varchar, author varchar, language varchar(2), date_yd TIMESTAMP_S, position integer, is_readed boolean, link_yd varchar)')
            return '[ОК] Таблица Book создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Book не создана. {e}'

    def make_fragment_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Fragment')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_fragment_id START 1;")
            self.base.execute("create table if not exists Fragment (id integer primary key default nextval('seq_fragment_id'), book_id integer REFERENCES Book(id), size integer, date_yd TIMESTAMP_S, date_analys TIMESTAMP_S, link_yd varchar)")
            return '[ОК] Таблица Fragment создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Fragment не создана. {e}'

    def make_quote_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Quote')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_quote_id START 1;")
            self.base.execute("create table if not exists Quote (id integer primary key default nextval('seq_quote_id'), fragment_id integer REFERENCES Fragment(id), text varchar, size integer, date_create TIMESTAMP_S, date_use TIMESTAMP_S)")
            return '[ОК] Таблица Quote создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Quote не создана. {e}'

    def make_voiceove_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Voiceover')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_voiceover_id START 1;")
            self.base.execute("create table if not exists Voiceover (id integer primary key default nextval('seq_voiceover_id'), quote_id integer REFERENCES Quote(id), type varchar, duration integer, date_create TIMESTAMP_S, date_use TIMESTAMP_S, link_yd VARCHAR)")
            return '[ОК] Таблица Voiceover создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Voiceover не создана. {e}'

    def load_books(self):
        try:
            print(f'[Процесс...] Загружаем книги')
            for item in self.y.listdir("app:/books"):
                if item.name.endswith('.txt'):
                    id, title, author, language = item.name.rstrip('.txt').split('_')
                    result = self.base.execute("select * from Book where id = ?", [id]).fetchall()
                    if not bool(result):
                        self.base.execute('insert into Book (id, title, author, language, date_yd, position, is_readed, link_yd) values(?, ?, ?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, 0, False, ?)', [int(id), title, author, language, f'app:/books/{item.name}'])
                        print(f'[ОК] Загружена книга {item.name}')
                    else:
                        print(f'[ОК] Книга {item.name} была загружена ранее')
                else:
                    print(f'[?] В папке нет книг')
        except Exception as e:
            print(f'[!] Ошибка! Книги не загружены. {e}')

    def make_book_fragment(self):

        def read_fragment_approx(file_path, byte_pos, chunk_bytes, encoding='utf-8'):
            """
            Читает chunk_bytes байт из файла начиная с byte_pos.
            Возвращает (текст, следующая_байтовая_позиция).
            """
            if byte_pos < 0:
                byte_pos = 0
            with open(str(file_path), 'rb') as f:
                f.seek(byte_pos)
                data = f.read(chunk_bytes)
                new_pos = f.tell()
                text = data.decode(encoding, errors='replace')
            return text, new_pos

        try:
            print(f'[Процесс...] Извлекаем фрагмент из книги')
            df = self.base.execute('SELECT * FROM Book WHERE is_readed IS FALSE ORDER BY id LIMIT 1').df()
            if df.empty:
                print(f'[Пусто] Все книги прочитаны')
                return
            book_object = df.iloc[0].to_dict()

            file_name = book_object['link_yd'].split('/')[-1]
            books_path = BASE_DIR / 'temp' / 'books' / file_name
            if not os.path.exists(books_path):
                self.y.download(book_object['link_yd'], str(books_path))

            text, next_byte = read_fragment_approx(books_path, book_object['position']-fragment_overlap * 2, fragment_size * 2)
            if text:
                cur_fragment = self.base.execute(
                    'insert into Fragment(book_id, size, date_yd, date_analys, link_yd) values (?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, Null, ?) RETURNING id',
                    [book_object['id'], len(text), None])
                fragment_id = cur_fragment.fetchone()[0]
                fragments_path = BASE_DIR / 'temp' / 'fragments' / f'{fragment_id}_{book_object["title"]}.txt'
                with open(fragments_path, 'w', encoding='utf-8') as fragment_file:
                    fragment_file.write(text)
                self.base.execute('update Fragment set link_yd = ? where id = ?',
                                  [f'app:/fragments/{fragment_id}_{book_object["title"]}.txt', fragment_id])
                self.y.upload(str(fragments_path),f'app:/fragments/{fragment_id}_{book_object["title"]}.txt')
                os.remove(fragments_path)
                print(f'[ОК] Получен фрагмент {fragment_id}')
                self.base.execute('update Book set position = ? where id = ?', [next_byte, book_object['id']])
            else:
                self.base.execute('update Book set is_readed = True where id = ?', [book_object['id']])
                print(f'[ОК] Книга прочитана до конца')
                os.remove(books_path)
        except Exception as e:
            print(f'[Ошибка] Фрагмент не получен: {e}')
            return False

    def analyse_fragment(self):
        df = self.base.execute('select * from Fragment where date_analys is Null limit 1').df()
        if df.empty:
            print(f'[Пусто] Нет непроанализированных фрагментов')
            return
        fragment_object = df.iloc[0].to_dict()
        print(f'[Процесс...] Анализируем фрагмент {fragment_object["id"]}')
        fragment_text = ''
        file_name = fragment_object['link_yd'].split('/')[-1]
        fragment_path = BASE_DIR / 'temp' / 'fragments' / file_name
        self.y.download(fragment_object['link_yd'], str(fragment_path))
        with open(fragment_path, 'r', encoding='utf-8') as fragment_file:
            fragment_text = fragment_file.read()

        client = InferenceClient(
            api_key=higging_face_token,
            timeout=30,  # ждём минуту
        )
        messages = [
            {"role": "system", "content": system_prompt_generate},
            {"role": "user", "content": f"Текст для анализа:\n\n{fragment_text}"}
        ]
        try:
            response = client.chat_completion(
                model=model_id,
                messages=messages,
                temperature=0.1,  # Низкая температура для точного извлечения без галлюцинаций
            )
            result = response.choices[0].message.content.strip()
            if result != 'NONE':
                cites_list = result.split('***')
                print(f'[ОК] Получен результат для фрагмента: {len(cites_list)} цитат. Проверяем их...')
                for i in range(len(cites_list)):
                    try:
                        messages = [
                            {"role": "system", "content": system_prompt_check},
                            {"role": "user", "content": f"Текст для анализа:\n\n{cites_list[i]}"}
                        ]
                        response = client.chat_completion(
                            model=model_id,
                            messages=messages,
                            temperature=0.1,  # Низкая температура для точного извлечения без галлюцинаций
                        )
                        result_check = response.choices[0].message.content.strip()
                        if result_check == 'NONE':
                            print(f'[ОК] Цитата {i+1} из фрагмента {fragment_object["id"]} отвергнута: {cites_list[i]}')
                        else:
                            quote_id = self.base.execute('insert into Quote(fragment_id, text, size, date_create, date_use) values (?, ?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, NULL) returning id', [fragment_object['id'], result_check, len(result_check.split())])
                            quote_id = quote_id.fetchone()[0]
                            print(f'[ОК] Цитата {i + 1}({quote_id}) из фрагмента {fragment_object["id"]} принята: {cites_list[i]}')
                    except Exception as e:
                        print(f'[Ошибка] Ошибка при обращении к API: {e}')

            else:
                print(f'[ОК] Проанализирован фрагмент {fragment_object["id"]}: {result}')
            self.base.execute('update Fragment set date_analys = NOW()::TIMESTAMP::TIMESTAMP_S where id = ?', [fragment_object['id']])
            os.remove(fragment_path)
        except Exception as e:
            print(f'[Ошибка] Ошибка при обращении к API: {e}')
            os.remove(fragment_path)
            return False

    def analyse_fragment_groq(self):
        df = self.base.execute('select * from Fragment where date_analys is Null limit 1').df()
        if df.empty:
            print(f'[Пусто] Нет непроанализированных фрагментов')
            return
        fragment_object = df.iloc[0].to_dict()
        print(f'[Процесс...] Анализируем фрагмент {fragment_object["id"]}')
        fragment_text = ''
        file_name = fragment_object['link_yd'].split('/')[-1]
        fragment_path = BASE_DIR / 'temp' / 'fragments' / file_name
        self.y.download(fragment_object['link_yd'], str(fragment_path))
        with open(fragment_path, 'r', encoding='utf-8') as fragment_file:
            fragment_text = fragment_file.read()

        client = Groq(api_key=groq_token)
        messages = [
            {"role": "system", "content": system_prompt_generate},
            {"role": "user", "content": f"Текст для анализа:\n\n{fragment_text}"}
        ]
        try:
            response = client.chat.completions.create(
                model=model_groq,
                messages=messages,
                temperature=0.1,  # Низкая температура для точного извлечения без галлюцинаций
            )
            result = response.choices[0].message.content.strip()
            if result != 'NONE':
                cites_list = result.split('***')
                print(f'[ОК] Получен результат для фрагмента: {len(cites_list)} цитат. Проверяем их...')
                for i in range(len(cites_list)):
                    if cites_list[i].strip():
                        try:
                            messages = [
                                {"role": "system", "content": system_prompt_check},
                                {"role": "user", "content": f"Текст для анализа:\n\n{cites_list[i]}"}
                            ]
                            response = client.chat.completions.create(
                                model=model_groq,
                                messages=messages,
                                temperature=0.1,  # Низкая температура для точного извлечения без галлюцинаций
                            )
                            result_check = response.choices[0].message.content.strip()
                            if result_check == 'NONE':
                                print(f'[ОК] Цитата {i+1} из фрагмента {fragment_object["id"]} отвергнута: {result_check}')
                            else:
                                quote_id = self.base.execute('insert into Quote(fragment_id, text, size, date_create, date_use) values (?, ?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, NULL) returning id', [fragment_object['id'], result_check, len(result_check.split())])
                                quote_id = quote_id.fetchone()[0]
                                print(f'[ОК] Цитата {i + 1}({quote_id}) из фрагмента {fragment_object["id"]} принята: {result_check}')
                        except Exception as e:
                            print(f'[Ошибка] Ошибка при обращении к API: {e}')

            else:
                print(f'[ОК] Нет цитат в {fragment_object["id"]}: {result}')
            self.base.execute('update Fragment set date_analys = NOW()::TIMESTAMP::TIMESTAMP_S where id = ?', [fragment_object['id']])
            os.remove(fragment_path)
        except Exception as e:
            print(f'[Ошибка] Ошибка при обращении к API: {e}')
            os.remove(fragment_path)
            return False

    def make_voiceover(self, voiceover_type, client):
        print(f'[Процесс...] Создаем озвучку цитаты')
        df = self.base.execute('''
                               SELECT Q.*
                               FROM Quote Q
                               WHERE NOT EXISTS (SELECT 1
                                                 FROM Voiceover V
                                                 WHERE V.quote_id = Q.id
                                                   AND V.type = ?)
                               ORDER BY Q.id LIMIT 1
                               ''', [voiceover_type]).df()
        if df.empty:
            print(f'[Пусто] Нет свободных цитат для этого типа озвучки')
            return
        quote_object = df.iloc[0].to_dict()
        print(f'[ОК] Найдена цитата {quote_object["id"]}: {quote_object["text"]}')

        try:
            voiceover_id = self.base.execute('insert into Voiceover(quote_id, type) values (?, ?) returning id', [quote_object['id'], voiceover_type])
            voiceover_id = voiceover_id.fetchone()[0]
            for attempt in range(7):
                try:
                    print(f"[Процесс...] Попытка {attempt + 1}: пробуем...")
                    result = client.predict(
                        quote_object["text"],
                        voiceover_id,
                        quote_object['id'],
                        voiceover_type,
                        "Сегодня на улице стоит прекрасная погода. Я занимаюсь настройкой нейронных сетей для автоматической озвучки текстов. Это очень интересный процесс, который требует внимания к деталям и правильной настройки всех параметров модели.",
                        api_name="/predict"
                    )
                    break
                except Exception as e:
                    print(f"[!] Попытка {attempt + 1}: Сервер еще не готов ({e})")
                    if attempt < 4:
                        time.sleep(10)
                    else:
                        print("[!!!] Критическая ошибка связи.")
            print(f"[ПК] Создана озвучка {result}")
            if self.y.exists(f'app:/voiceovers/{voiceover_id}_{quote_object["id"]}_{voiceover_type}'):
                self.base.execute('update Voiceover set date_create = NOW()::TIMESTAMP::TIMESTAMP_S, link_yd = ? where id = ?', [f'app:/voiceovers/{voiceover_id}_{quote_object["id"]}_{voiceover_type}', voiceover_id])
            else:
                self.base.execute('delete from Voiceover where id = ?', [voiceover_id])
                print(f"[Ошибка] Файл отсутствует на яндекс-диске: {e}")
        except Exception as e:
            self.base.execute('delete from Voiceover where id = ?', [voiceover_id])
            print(f"[Ошибка] {e}")

    def run_voiceover(self, n, type):
        client = run_kaggle_notebook('gradio_url_voiceover.txt', "tim3la/voiceover-1", 'C:/python_projects_tim3la/content_farm/notebooks/voiceover')
        if client:
            for i in range(n):
                main_db.make_voiceover(type, client)
        else:
            print("[!] Не удалось запустить Kaggle-сервер. Прерываем работу.")


if __name__ == '__main__':
    main_db = Main_DB(db_name, yd_token)
    main_db.load_books()
    # for i in range(3):
    #     main_db.make_book_fragment()
    #     main_db.analyse_fragment_groq()
    main_db.run_voiceover(7, 1)
