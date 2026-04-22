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
import traceback


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
    "7. предложения в тексте не повторяются"
)
system_prompt_check = (
    "Проверь этот текст по следующим параметрам:"
    "Текст представляет собой законченную мысль, он не обрывается на полуслове. Объем текста - до 200 слов. Текст состоит только из русских слов. Текст содержит в себе интересную мысль, философское изречение или жизненное наблюдение, или рассуждение. Текст понятен без контекста и не оставляет вопросов: о ком или о чем идет речь? В нем не упоминаются непонятные имена. предложения в тексте не повторяются"
    "Если текст соответствует этим требованиям, отредактируй его: убери лишние слова в скобках, какие-то лишние пометки и комментарии, которые бы диктор не стал читать. В результате выведи только этот отредактированный текст. Только сам текст, без твоих комментариев!"
    "Если текст не соответствует требованиям, выведи только слово NONE"
)
system_prompt_img = (
    "Сгенерируй промпт для атмосферной картинки в стиле мрачной средневековой фантастики. Четко следуй следующей текстуре промпта: "
    "слово 'dark-fantazy-medieval-aesthetics-style.', кто? что делает? около чего? (не обязательно) в какое время суток? в какой местности? что находится на фоне? сторона, с которой вид (например сзади, сбоку"
    "спереди и т.д.) дальность вида (близкий, далекий, средний и т.д). Цвет освещения"
    "Полученный промпт не должен быть больше, чем 354 символа. Для описания используй факты, не используй оценочные прилагательные. участником картинки могут быть: рыцари, принцессы, колдуны, скелеты, мифические существа, животные и так далее"
    "Вот примеры:"
    """dark-fantazy-medieval-aesthetics-style. Knights sitting by a fire at night in the forest, with the moon and a castle in the background, front view close. The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. A knight sitting on a wasteland at night, with a bright starry sky in the background, front view. The lighting is blue

dark-fantazy-medieval-aesthetics-style. Wizards stand at sunset on the edge of a cliff, with a castle in the background, a bright sky, mountains, and fog. view far from behind. The lighting is purple-yellow

dark-fantazy-medieval-aesthetics-style. A knight on a horse is walking on a lawn in the forest in the evening, with a castle, fields, fog, and trees in the background. View far from behind. The lighting is dark green

dark-fantazy-medieval-aesthetics-style. Knight lying at night on a stone near flowers near a tree in the forest, at night, in the background fog, mountains, red sky. Front view close. The lighting is red

dark-fantazy-medieval-aesthetics-style. a knight on a horse is running near a mountain of skeletons at night among the mountains, with a castle, fog, glow, mountains, and trees in the background. the view from behind is far away. The lighting is green

dark-fantazy-medieval-aesthetics-style. a knight on a horse is standing near the water at night in the middle of a forest, with a glowing moon and trees in the background. the view from the front is far away

dark-fantazy-medieval-aesthetics-style.wizards stand high in the mountains during the day, with the sun and mountains in the background. side view far away. The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. a wizard stands near an igloo in the mountains during the day in winter, with a mountain in the background. side view far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style.a wizard is sitting in a castle window at night, with a mountain, moon, castle, city, and fog in the background. the view from behind is far away. The lighting is dark blue

dark-fantazy-medieval-aesthetics-style. A knight lies in a clearing at night, with a bright starry sky and a glow in the background. Close-up view from the side. The lighting is blue

dark-fantazy-medieval-aesthetics-style. castle near the abyss at sunset, with a thunderstorm and fog in the background. the view from the front is far away. The lighting is purple-pink

dark-fantazy-medieval-aesthetics-style. a knight climbs the stairs to the castle at night in the thicket, with the castle in the background and fog. the view from behind is far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style. wizards walking in the mountain forest to the castle in the evening, castle background, big moon, fog. view from behind far away. The lighting is blue-orange

dark-fantazy-medieval-aesthetics-style.castle on the mountaintop at sunset, with bright clouds in the background. the view from the front is far away. the lighting is green-orange

dark-fantazy-medieval-aesthetics-style.castle in the middle of the mountains at night in winter, with the moon in the background. front view far away.

dark-fantazy-medieval-aesthetics-style. a skeleton standing in a cave in the afternoon near rocks, with rocks and trees in the background. close-up view from the front.  The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. A knight and a princess are standing in a castle near flowers in the afternoon. Flowers are in the background. Close-up view from the side. The lighting is yellow

dark-fantazy-medieval-aesthetics-style. a knight holds a princess in the evening near the grass. in the background is a forest. a close-up view from the front

dark-fantazy-medieval-aesthetics-style. a wizard is sitting near a river in the forest in the evening. in the background, there is a castle, trees, and fog. the view from the front is far away. the lighting is yellow-green

dark-fantazy-medieval-aesthetics-style. a wizard is sitting near a river in the forest in the evening. in the background, there is a castle, trees, a mountain, and fog. the view from the front is far away. the lighting is yellow-green

dark-fantazy-medieval-aesthetics-style. castle among the mountains at night, with a glowing moon in the background. front view far away. The lighting is red

dark-fantazy-medieval-aesthetics-style. a skeleton sitting on a staircase near a candle at night, with a castle, mountains, fog, glow, and a distant view in the background. The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. a knight is sitting near a lake in a swamp at sunset, with a castle in the background, a glowing sky, trees, fog, and a close view from the front. The lighting is pink. 

dark-fantazy-medieval-aesthetics-style. castle in the mountains near a river at night, with the moon and mountains in the background. front view far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style. house in the forest at night near a path, background trees, fog, view from the front far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style. a girl lying on a dragon near the rocks in the evening, against the background of a mountain, a thunderstorm, a glow, a close view from the front. The lighting is yellow

dark-fantazy-medieval-aesthetics-style.a knight holds a cat near flowers in the afternoon, with a forest, flowers, and clouds in the background. close-up view from the front. The lighting is yellow

dark-fantazy-medieval-aesthetics-style.a knight and a girl sitting on a mountain at night in winter, with a mountain in the background, a bright sky, and a close view from behind. The lighting is blue

dark-fantazy-medieval-aesthetics-style. a knight and a girl walking through a flower field in the afternoon, with a forest, a castle, and clouds in the background. close-up view from behind.The lighting is white"""
    "На основе этих примеров придумай что то подобное. Экспериментируй с цветами и сценами. Напиши на английском языке. Не добавляй свои комментарии, напиши только промпт. Напиши только один промпт, То есть одна сцена"
)

system_prompt_img_many = (
    "Сгенерируй 5 промптов для атмосферной картинки в стиле мрачной средневековой фантастики. Четко следуй следующей текстуре промпта: "
    "кто? в какой одежде? что делает? около чего? (не обязательно) в какое время суток? в какой местности? что находится на фоне? сторона, с которой вид (например сзади, сбоку"
    "спереди и т.д.) дальность вида (близкий, далекий, средний и т.д). Цвет освещения"
    "Полученные промпты не должен быть больше, чем 354 символа каждый. Для описания используй факты, не используй оценочные прилагательные. участником картинки могут быть: рыцари, принцессы, колдуны, скелеты, мифические существа, животные и так далее"
    "Вот примеры:"
    """dark-fantazy-medieval-aesthetics-style. Knights sitting by a fire at night in the forest, with the moon and a castle in the background, front view close. The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. A knight sitting on a wasteland at night, with a bright starry sky in the background, front view. The lighting is blue

dark-fantazy-medieval-aesthetics-style. Wizards stand at sunset on the edge of a cliff, with a castle in the background, a bright sky, mountains, and fog. view far from behind. The lighting is purple-yellow

dark-fantazy-medieval-aesthetics-style. A knight on a horse is walking on a lawn in the forest in the evening, with a castle, fields, fog, and trees in the background. View far from behind. The lighting is dark green

dark-fantazy-medieval-aesthetics-style. Knight lying at night on a stone near flowers near a tree in the forest, at night, in the background fog, mountains, red sky. Front view close. The lighting is red

dark-fantazy-medieval-aesthetics-style. a knight on a horse is running near a mountain of skeletons at night among the mountains, with a castle, fog, glow, mountains, and trees in the background. the view from behind is far away. The lighting is green

dark-fantazy-medieval-aesthetics-style. a knight on a horse is standing near the water at night in the middle of a forest, with a glowing moon and trees in the background. the view from the front is far away

dark-fantazy-medieval-aesthetics-style.wizards stand high in the mountains during the day, with the sun and mountains in the background. side view far away. The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. a wizard stands near an igloo in the mountains during the day in winter, with a mountain in the background. side view far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style.a wizard is sitting in a castle window at night, with a mountain, moon, castle, city, and fog in the background. the view from behind is far away. The lighting is dark blue

dark-fantazy-medieval-aesthetics-style. A knight lies in a clearing at night, with a bright starry sky and a glow in the background. Close-up view from the side. The lighting is blue

dark-fantazy-medieval-aesthetics-style. castle near the abyss at sunset, with a thunderstorm and fog in the background. the view from the front is far away. The lighting is purple-pink

dark-fantazy-medieval-aesthetics-style. a knight climbs the stairs to the castle at night in the thicket, with the castle in the background and fog. the view from behind is far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style. wizards walking in the mountain forest to the castle in the evening, castle background, big moon, fog. view from behind far away. The lighting is blue-orange

dark-fantazy-medieval-aesthetics-style.castle on the mountaintop at sunset, with bright clouds in the background. the view from the front is far away. the lighting is green-orange

dark-fantazy-medieval-aesthetics-style.castle in the middle of the mountains at night in winter, with the moon in the background. front view far away.

dark-fantazy-medieval-aesthetics-style. a skeleton standing in a cave in the afternoon near rocks, with rocks and trees in the background. close-up view from the front.  The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. A knight and a princess are standing in a castle near flowers in the afternoon. Flowers are in the background. Close-up view from the side. The lighting is yellow

dark-fantazy-medieval-aesthetics-style. a knight holds a princess in the evening near the grass. in the background is a forest. a close-up view from the front

dark-fantazy-medieval-aesthetics-style. a wizard is sitting near a river in the forest in the evening. in the background, there is a castle, trees, and fog. the view from the front is far away. the lighting is yellow-green

dark-fantazy-medieval-aesthetics-style. a wizard is sitting near a river in the forest in the evening. in the background, there is a castle, trees, a mountain, and fog. the view from the front is far away. the lighting is yellow-green

dark-fantazy-medieval-aesthetics-style. castle among the mountains at night, with a glowing moon in the background. front view far away. The lighting is red

dark-fantazy-medieval-aesthetics-style. a skeleton sitting on a staircase near a candle at night, with a castle, mountains, fog, glow, and a distant view in the background. The lighting is turquoise

dark-fantazy-medieval-aesthetics-style. a knight is sitting near a lake in a swamp at sunset, with a castle in the background, a glowing sky, trees, fog, and a close view from the front. The lighting is pink. 

dark-fantazy-medieval-aesthetics-style. castle in the mountains near a river at night, with the moon and mountains in the background. front view far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style. house in the forest at night near a path, background trees, fog, view from the front far away. The lighting is blue

dark-fantazy-medieval-aesthetics-style. a girl lying on a dragon near the rocks in the evening, against the background of a mountain, a thunderstorm, a glow, a close view from the front. The lighting is yellow

dark-fantazy-medieval-aesthetics-style.a knight holds a cat near flowers in the afternoon, with a forest, flowers, and clouds in the background. close-up view from the front. The lighting is yellow

dark-fantazy-medieval-aesthetics-style.a knight and a girl sitting on a mountain at night in winter, with a mountain in the background, a bright sky, and a close view from behind. The lighting is blue

dark-fantazy-medieval-aesthetics-style. a knight and a girl walking through a flower field in the afternoon, with a forest, a castle, and clouds in the background. close-up view from behind.The lighting is white"""
    "На основе этих примеров придумай что то подобное. Экспериментируй с цветами и сценами. Напиши на английском языке. Не добавляй свои комментарии. Каждый промпт отделяй знаком '***'. Все промпты должны быть кардинально разными: разное освещение, разные персонажи, разные сцены"
)
system_prompt_img_many = (
    "Сгенерируй 5 промптов для атмосферной картинки в стиле мрачной средневековой фантастики. "
    "Четко следуй следующей текстуре промпта: "
    "кто? в какой одежде? что делает? около чего? (не обязательно) в какое время суток? в какой местности? что находится на фоне? сторона, с которой вид (например сзади, сбоку"
    "спереди и т.д.) дальность вида (близкий, далекий, средний и т.д). Цвет освещения"
    "ВАЖНО: Порядок элементов в каждом из 5 промптов должен быть РАЗНЫМ. Иногда начинай с локации, иногда с действия, иногда с освещения. Не используй шаблон 'A wizard is standing' чаще одного раза на 5 промптов."
    "ВАЖНО: Вместо глаголов состояния (is standing, is sitting) используй глаголы действия (lurks, rests, gazes, trudges) или причастия (seated, perched, cloaked)."
    "ВАЖНО: Цвет освещения вставляй в описание, а не отдельным предложением, если это возможно (например 'bathed in cold turquoise moonlight' вместо 'The lighting is turquoise')."
    "Полученные промпты не должен быть больше, чем 354 символа каждый. Для описания используй факты, не используй оценочные прилагательные. участником картинки могут быть: рыцари, принцессы, колдуны, скелеты, мифические существа, животные и так далее"
    "Вот примеры (обрати внимание, они имеют разную структуру, не только 'A кто-то is...'):"
    """ Knights sitting by a fire at night in the forest, with the moon and a castle in the background, front view close. The lighting is turquoise
. A knight sitting on a wasteland at night, with a bright starry sky in the background, front view. The lighting is blue
. Wizards stand at sunset on the edge of a cliff, with a castle in the background, a bright sky, mountains, and fog. view far from behind. The lighting is purple-yellow

 A knight on a horse is walking on a lawn in the forest in the evening, with a castle, fields, fog, and trees in the background. View far from behind. The lighting is dark green
. Knight lying at night on a stone near flowers near a tree in the forest, at night, in the background fog, mountains, red sky. Front view close. The lighting is red

 a knight on a horse is running near a mountain of skeletons at night among the mountains, with a castle, fog, glow, mountains, and trees in the background. the view from behind is far away. The lighting is green

 a knight on a horse is standing near the water at night in the middle of a forest, with a glowing moon and trees in the background. the view from the front is far away

wizards stand high in the mountains during the day, with the sun and mountains in the background. side view far away. The lighting is turquoise

 a wizard stands near an igloo in the mountains during the day in winter, with a mountain in the background. side view far away. The lighting is blue

a wizard is sitting in a castle window at night, with a mountain, moon, castle, city, and fog in the background. the view from behind is far away. The lighting is dark blue

 A knight lies in a clearing at night, with a bright starry sky and a glow in the background. Close-up view from the side. The lighting is blue
. castle near the abyss at sunset, with a thunderstorm and fog in the background. the view from the front is far away. The lighting is purple-pink

 a knight climbs the stairs to the castle at night in the thicket, with the castle in the background and fog. the view from behind is far away. The lighting is blue

 wizards walking in the mountain forest to the castle in the evening, castle background, big moon, fog. view from behind far away. The lighting is blue-orange

castle on the mountaintop at sunset, with bright clouds in the background. the view from the front is far away. the lighting is green-orange

castle in the middle of the mountains at night in winter, with the moon in the background. front view far away.

 a skeleton standing in a cave in the afternoon near rocks, with rocks and trees in the background. close-up view from the front.  The lighting is turquoise

 A knight and a princess are standing in a castle near flowers in the afternoon. Flowers are in the background. Close-up view from the side. The lighting is yellow

 a knight holds a princess in the evening near the grass. in the background is a forest. a close-up view from the front

 a wizard is sitting near a river in the forest in the evening. in the background, there is a castle, trees, and fog. the view from the front is far away. the lighting is yellow-green

 a wizard is sitting near a river in the forest in the evening. in the background, there is a castle, trees, a mountain, and fog. the view from the front is far away. the lighting is yellow-green

 castle among the mountains at night, with a glowing moon in the background. front view far away. The lighting is red

 a skeleton sitting on a staircase near a candle at night, with a castle, mountains, fog, glow, and a distant view in the background. The lighting is turquoise

 a knight is sitting near a lake in a swamp at sunset, with a castle in the background, a glowing sky, trees, fog, and a close view from the front. The lighting is pink. 

 a castle in the mountains near a river at night, with the moon and mountains in the background. front view far away. The lighting is blue

 a house in the forest at night near a path, background trees, fog, view from the front far away. The lighting is blue

 a girl lying on a dragon near the rocks in the evening, against the background of a mountain, a thunderstorm, a glow, a close view from the front. The lighting is yellow

a knight holds a cat near flowers in the afternoon, with a forest, flowers, and clouds in the background. close-up view from the front. The lighting is yellow

a knight and a girl sitting on a mountain at night in winter, with a mountain in the background, a bright sky, and a close view from behind. The lighting is blue
. a knight and a girl walking through a flower field in the afternoon, with a forest, a castle, and clouds in the background. close-up view from behind.The lighting is white"""
    "На основе этих примеров придумай что то подобное. Экспериментируй с цветами и сценами. Напиши на английском языке. Не добавляй свои комментарии. Каждый промпт отделяй знаком '***'. Все промпты должны быть кардинально разными: разное освещение, разные персонажи, разные сцены"
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
        print(self.make_prompt_img_table())
        print(self.make_img_table())
        print(self.make_video_table())

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

    def make_prompt_img_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Prompt_img')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_prompt_img_id START 1;")
            self.base.execute("create table if not exists Prompt_img (id integer primary key default nextval('seq_prompt_img_id'), text varchar, size integer,  date_create TIMESTAMP_S, date_last_use TIMESTAMP_S, use_number integer)")
            return '[ОК] Таблица Prompt_img создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Prompt_img не создана. {e}'

    def make_img_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Image')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_image_id START 1;")
            self.base.execute("create table if not exists Image (id integer primary key default nextval('seq_image_id'), prompt_id integer REFERENCES Prompt_img(id), size integer,  date_create TIMESTAMP_S, date_last_use TIMESTAMP_S, link_yd varchar)")
            return '[ОК] Таблица Image создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Image не создана. {e}'

    def make_video_table(self):
        try:
            print(f'[Процесс...] Создаем таблицу Video')
            self.base.execute("CREATE SEQUENCE IF NOT EXISTS seq_video_id START 1;")
            self.base.execute("create table if not exists Video (id integer primary key default nextval('seq_video_id'), img_id integer REFERENCES Image(id), duration integer, date_create TIMESTAMP_S, date_last_use TIMESTAMP_S, link_yd varchar)")
            return '[ОК] Таблица Video создана'
        except Exception as e:
            return f'[!] Ошибка! Таблица Video не создана. {e}'

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
                        api_name="/gen"
                    )
                    break
                except Exception as e:
                    print(f"[!] Попытка {attempt + 1}: Сервер еще не готов ({e})")
                    if attempt < 4:
                        time.sleep(10)
                    else:
                        print("[!!!] Критическая ошибка связи.")
            print(f"[ПК] Создана озвучка {result}")
            if self.y.exists(f'app:/voiceovers/{voiceover_id}_{quote_object["id"]}_{voiceover_type}.wav'):
                self.base.execute('update Voiceover set date_create = NOW()::TIMESTAMP::TIMESTAMP_S, link_yd = ? where id = ?', [f'app:/voiceovers/{voiceover_id}_{quote_object["id"]}_{voiceover_type}.wav', voiceover_id])
            else:
                self.base.execute('delete from Voiceover where id = ?', [voiceover_id])
                print(f"[Ошибка] Файл отсутствует на яндекс-диске: {e}")
        except Exception as e:
            self.base.execute('delete from Voiceover where id = ?', [voiceover_id])
            print(f"[Ошибка] {e}")

    def run_voiceover(self, n, type):
        client = run_kaggle_notebook('gradio_url_voiceover.txt', "tim3la/voiceover-1", str(BASE_DIR / 'notebooks' / 'voiceover'))
        if client:
            for i in range(n):
                main_db.make_voiceover(type, client)
        else:
            print("[!] Не удалось запустить Kaggle-сервер. Прерываем работу.")
        try:
            client.predict(api_name="/stop_server")
        except Exception:
            pass  # сервер УМЕР — это ожидаемо

    def make_img_prompt(self):
        print(f'[Процесс...] Генерируем промпт для картинки')
        client = Groq(api_key=groq_token)
        messages = [
            {"role": "system", "content": system_prompt_img},
        ]
        try:
            response = client.chat.completions.create(
                model=model_groq,
                messages=messages,
                temperature=0.9,  # высокая  температура для креативности
            )
            result = response.choices[0].message.content.strip()
            print(f'[Процесс...] Промпт: {result}')
            self.base.execute('insert into Prompt_img(text, size, date_create, use_number) values (?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, 0)', [result, len(result)])
        except Exception as e:
            print(f'[Ошибка] Проблема генерации промпта: {e}')

    def make_img_prompt_many(self):
        print(f'[Процесс...] Генерируем промпт для картинки')
        client = Groq(api_key=groq_token)
        messages = [
            {"role": "system", "content": system_prompt_img_many},
        ]
        try:
            response = client.chat.completions.create(
                model=model_groq,
                messages=messages,
                temperature=0.9,  # Низкая температура для точного извлечения без галлюцинаций
            )
            result = response.choices[0].message.content.strip().split('***')
            print(f'[Процесс...] Промпт: {result}')
            for e in result:
                if e.strip():
                    self.base.execute('insert into Prompt_img(text, size, date_create, use_number) values (?, ?, NOW()::TIMESTAMP::TIMESTAMP_S, 0)', [e.strip(), len(e)])
        except Exception as e:
            print(f'[Ошибка] Проблема генерации промпта: {e}')

    def make_img(self, client):
        print(f'[Процесс...] Создаем картинку')
        df = self.base.execute('select * from Prompt_img order by use_number, date_last_use, id limit 1').df()
        if df.empty:
            print(f'[Пусто] Нет промптов')
            return
        prompt_object = df.iloc[0].to_dict()
        print(f'[ОК] Найден промпт {prompt_object["id"]}: {prompt_object["text"]}')

        try:
            img_id = self.base.execute('insert into Image(prompt_id) values (?) returning id',
                                             [prompt_object['id']])
            img_id = img_id.fetchone()[0]
            for attempt in range(7):
                try:
                    print(f"[Процесс...] Попытка {attempt + 1}: пробуем...")
                    result = client.predict(
                        img_id,  # id
                        prompt_object["id"],  # prompt_id
                        prompt_object["text"] + 'dark fantasy style, cinematic, realistic, photo',  # prompt
                        1.2,
                        None,
                        api_name="/gen"
                    )
                    print(f'Результат = {result}')
                    break
                except Exception as e:
                    print(f"[!] Попытка {attempt + 1}: Сервер еще не готов ({e})")
                    if attempt < 4:
                        time.sleep(10)
                    else:
                        print("[!!!] Критическая ошибка связи.")
            print(f"[ПК] Создана картинка {img_id}_{prompt_object['id']}.png")
            if self.y.exists(f'app:/images/{img_id}_{prompt_object['id']}.png'):
                self.base.execute(
                    'update Image set date_create = NOW()::TIMESTAMP::TIMESTAMP_S, link_yd = ? where id = ?',
                    [f'app:/images/{img_id}_{prompt_object['id']}.png', img_id])
                self.base.execute('update Prompt_img set date_last_use=NOW()::TIMESTAMP::TIMESTAMP_S, use_number = ? where id = ?', [prompt_object['use_number'] + 1, prompt_object['id']])
            else:
                self.base.execute('delete from Image where id = ?', [img_id])
                print(f"[Ошибка] Файл отсутствует на яндекс-диске: {e}")
        except Exception as e:
            self.base.execute('delete from Image where id = ?', [img_id])
            print(f"[Ошибка] {e}")

    def run_make_img(self, n):
        client = run_kaggle_notebook('gradio_url_image.txt', "tim3la/image-1", str(BASE_DIR / 'notebooks' / 'image'))
        if client:
            print('[ОК] Успешное подключение к kaggle')
            for i in range(n):
                print(f'[Процесс...] Картинка {i+1} из {n}')
                main_db.make_img(client)
        else:
            print("[!] Не удалось запустить Kaggle-сервер. Прерываем работу.")
        try:
            client.predict(api_name="/stop_server")
        except Exception:
            pass  # сервер УМЕР — это ожидаемо

    def make_video(self, client):
        print(f'[Процесс...] Создаем видео')
        # df = self.base.execute('select * from Image where date_last_use is null order by id limit 1').df()
        df = self.base.execute('select * from Image where date_last_use is null order by id limit 1').df()
        if df.empty:
            print(f'[Пусто] Нет картинок')
            return
        img_object = df.iloc[0].to_dict()
        print(f'[ОК] Найдена картинка {img_object["id"]}: {img_object["link_yd"]}')

        try:
            video_id = self.base.execute('insert into Video(img_id) values (?) returning id',
                                       [img_object['id']])
            video_id = video_id.fetchone()[0]
            for attempt in range(7):
                try:
                    print(f"[Процесс...] Попытка {attempt + 1}: пробуем...")
                    result = client.predict(
                        video_id,  # id
                        img_object["id"],  # prompt_id
                        img_object["link_yd"],
                        api_name="/gen"
                    )
                    print(f'Результат = {result}')
                    break
                except Exception as e:
                    print(f"[!] Попытка {attempt + 1}: Сервер еще не готов ({e})")
                    if attempt < 4:
                        time.sleep(10)
                    else:
                        print("[!!!] Критическая ошибка связи.")
            print(f"[ПК] Создано видео {video_id}_{img_object['id']}.mp4")
            if self.y.exists(f'app:/videos/{video_id}_{img_object['id']}.mp4'):
                self.base.execute(
                    'update Video set date_create = NOW()::TIMESTAMP::TIMESTAMP_S, link_yd = ? where id = ?',
                    [f'app:/videos/{video_id}_{img_object['id']}.mp4', video_id])
                self.base.execute(
                    'update Image set date_last_use=NOW()::TIMESTAMP::TIMESTAMP_S where id = ?',
                    [img_object['id']])
            else:
                self.base.execute('delete from Video where id = ?', [video_id])
                print(f"[Ошибка] Файл отсутствует на яндекс-диске: {e}")
        except Exception as e:
            self.base.execute('delete from Video where id = ?', [video_id])
            print(f"[Ошибка] {e}")

    def run_make_video(self, n):
        client = run_kaggle_notebook('gradio_url_video.txt', "tim3la/video-1", str(BASE_DIR / 'notebooks' / 'video'), False)
        if client:
            print('[ОК] Успешное подключение к kaggle')
            for i in range(n):
                print(f'[Процесс...] Картинка {i + 1} из {n}')
                main_db.make_video(client)
        else:
            print("[!] Не удалось запустить Kaggle-сервер. Прерываем работу.")
        try:
            client.predict(api_name="/stop_server")
        except Exception:
            pass  # сервер УМЕР — это ожидаемо

if __name__ == '__main__':
    main_db = Main_DB(db_name, yd_token)
    main_db.load_books()
    # for i in range(7):
    #     main_db.make_book_fragment()
    #     main_db.analyse_fragment_groq()
    # main_db.run_voiceover(7, 1)
    # for i in range(3):
    #     main_db.make_img_prompt_many()
    #     time.sleep(10)
    # main_db.run_make_img(2)
    main_db.run_make_video(15)
    print('Готово!')