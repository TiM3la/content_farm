import os.path
from math import ceil
from generation_lib import *
from moviepy import VideoFileClip, AudioFileClip, concatenate_videoclips, TextClip, CompositeVideoClip, ImageClip, CompositeAudioClip
from moviepy.audio.fx import AudioFadeOut
from moviepy.video.fx import FadeOut, TimeMirror, Margin
from moviepy.video.tools.subtitles import SubtitlesClip
import random as r
import textwrap
from PIL import Image, ImageDraw, ImageFont



TYPES_OF_VIDEO_CONTENT = {
    1: "Склейка клипов под dark fantasy музыку",
    2: "Склейка клипов под dark fantasy музыку с озвучкой",
    3: "Склейка клипов под dark fantasy музыку с мистическим вопросом"
}

TYPES_OF_CAROUSEL_CONTENT = {
    1: "Слайд-шоу фото под dark fantasy музыку",
    2: "Слайд-шоу фото под dark fantasy музыку с надписью 'обои на телефон' или авы",
    3: "Слайд-шоу фото под dark fantasy музыку с частями текста на каждом слайде"
}

UNIT_DURATION = {
    'video': 3,
    'voiceover_word': 0.4
}

class Video_task:
    def __init__(self, type, duration_s):
        self.type = type
        self.title = TYPES_OF_VIDEO_CONTENT[type]
        self.duration_s = duration_s
        self.num_of_videos = ceil(self.duration_s / UNIT_DURATION['video'])
        self.voiceover_object = None

        if self.type == 2:
            self.voiceover_words = int(self.duration_s / UNIT_DURATION['voiceover_word'])

        if self.type == 3:
            self.num_of_videos = 1

    def get_task_id(self):
        if self.type == 1:
            video_task_dict = {
                'type': 1,
                'title': self.title,
                'duration': self.duration_s,
                'video_num': self.num_of_videos,
                'music_id': self.music_object['id'],
                'voiceover_id': None,
                'video_list': [video_object['id'] for video_object in self.video_objects]
            }
        if self.type == 2:
            video_task_dict = {
                'type': 2,
                'title': self.title,
                'duration': self.duration_s,
                'video_num': self.num_of_videos,
                'music_id': self.music_object['id'],
                'voiceover_id': self.voiceover_object['id'],
                'video_list': [video_object['id'] for video_object in self.video_objects]
            }
        if self.type == 3:
            video_task_dict = {
                'type': 3,
                'title': self.title,
                'duration': self.duration_s,
                'video_num': self.num_of_videos,
                'music_id': self.music_object['id'],
                'voiceover_id': None,
                'video_list': [video_object['id'] for video_object in self.video_objects]
            }
        self.video_task_id = main_db.write_video_task(video_task_dict)
        return self.video_task_id

    def find_resources(self):
        if self.type == 1:
            self.video_objects = main_db.find_clips(self.num_of_videos)
            self.music_object = main_db.find_music() # условие - не тревожная музыка
             # print(self.video_objects)
            # print(self.music_object)
        if self.type == 2:
            self.voiceover_object, self.duration_s = main_db.find_voiceover(self.duration_s)
            self.num_of_videos = ceil(self.duration_s / UNIT_DURATION['video'])
            self.video_objects = main_db.find_clips(self.num_of_videos)
            self.music_object = main_db.find_music()
            self.subtitle_object = main_db.get_info(f'select * from Subtitle where voiceover_id = {self.voiceover_object["id"]}')

            print(self.voiceover_object, self.video_objects, self.music_object)

        if self.type == 3:
            self.video_objects = main_db.find_clips(self.num_of_videos)
            self.music_object = main_db.find_music('id = 1')

            print(self.video_objects[0]['id'])
            self.mistery_question = main_db.get_info(f'select p.* from Prompt_img as p inner join Image as i on i.prompt_id = p.id inner join Video as v on v.img_id = i.id where v.id = {self.video_objects[0]['id']}')['mistery_question']

            print(self.video_objects)
            print(self.music_object)
            print(self.mistery_question)

        self.get_task_id()

    def download_resources(self):
        for video_object in self.video_objects:
            main_db.y.download(video_object['link_yd'], str(BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd'])))

        main_db.y.download(self.music_object['link_yd'], str(BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])))

        if self.type == 2:
            main_db.y.download(self.voiceover_object['link_yd'], str(BASE_DIR / 'temp' / 'voiceovers' / os.path.basename(self.voiceover_object['link_yd'])))
            main_db.y.download(self.subtitle_object['link_yd'], str(BASE_DIR / 'temp' / 'subtitles' / os.path.basename(self.subtitle_object['link_yd'])))

    def clear_resources(self):
        # удаляем озвучку
        if self.voiceover_object:
            voiceover_path = BASE_DIR / 'temp' / 'voiceovers' / os.path.basename(self.voiceover_object['link_yd'])
            if voiceover_path.exists():
                os.remove(voiceover_path)
        # удаляем субтитры
        if self.subtitle_object:
            subtitle_path = BASE_DIR / 'temp' / 'subtitles' / os.path.basename(self.subtitle_object['link_yd'])
            if subtitle_path.exists():
                os.remove(subtitle_path)

        # Удаляем видеофайлы
        for video_object in self.video_objects:
            video_path = BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd'])
            if video_path.exists():
                os.remove(video_path)
        # Удаляем музыку
        music_path = BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])
        if music_path.exists():
            os.remove(music_path)
        # удаляем это видео
        video_task_path = BASE_DIR / 'temp' / 'video_tasks' / f'{self.video_task_id}_{self.type}.mp4'
        if video_task_path.exists():
            os.remove(video_task_path)

    def make_video_type_1(self):
        # загружаем видео и музыку
        clips = []
        music = None
        final_video = None

        try:
            for video_object in self.video_objects:
                clips.append(
                    VideoFileClip(str(BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd']))))
            music = AudioFileClip(
                str(BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])))
            # балансируем громкость
            music = set_rms(music, 0.2)

            # склеиваем
            final_video = concatenate_videoclips(clips)

            # обрезаем видео под длительность
            final_video = final_video.subclipped(0, self.duration_s)

            # Если музыка длиннее итогового видео — обрезаем
            if music.duration > final_video.duration:
                music = music.subclipped(0, final_video.duration)

            # делаем затухание музыки
            music = music.with_effects([AudioFadeOut(1)])

            # делаем затухание видео
            final_video = final_video.with_effects([FadeOut(1)])

            # Заменяем оригинальную аудиодорожку видео на музыку
            final_video = final_video.with_audio(music)

            # Сохраняем результат
            final_video.write_videofile(
                str(BASE_DIR / 'temp' / 'video_tasks' / f"{self.video_task_id}_{self.type}.mp4"),
                codec="libx264", audio_codec="aac")

        finally:
            # Закрываем все клипы, чтобы освободить файлы
            for clip in clips:
                clip.close()
            if music is not None:
                music.close()
            if final_video is not None:
                final_video.close()

    def make_video_type_2(self):
        # загружаем видео, музыку, озвучку
        clips = []
        music = None
        voiceover = None
        final_video = None
        subs_clip = None

        try:
            for video_object in self.video_objects:
                clips.append(
                    VideoFileClip(str(BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd']))))
            music = AudioFileClip(
                str(BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])))

            voiceover = AudioFileClip(BASE_DIR / 'temp' / 'voiceovers' / os.path.basename(self.voiceover_object['link_yd']))

            # балансируем громкость
            music = set_rms(music, 0.1)
            voiceover = set_rms(voiceover, 0.2)

            # склеиваем
            final_video = concatenate_videoclips(clips)

            # обрезаем видео под длительность
            final_video = final_video.subclipped(0, self.duration_s)

            # Если музыка длиннее итогового видео — обрезаем
            if music.duration > final_video.duration:
                music = music.subclipped(0, final_video.duration)

            # делаем затухание музыки
            music = music.with_effects([AudioFadeOut(1)])
            voiceover = voiceover.with_effects([AudioFadeOut(1)])

            # делаем затухание видео
            final_video = final_video.with_effects([FadeOut(1)])

            # Заменяем оригинальную аудиодорожку видео на музыку
            final_video = final_video.with_audio(CompositeAudioClip([music, voiceover]))

            # === НОВАЯ ГРУППИРОВКА СЛОВ ===
            sub_path = str(BASE_DIR / 'temp' / 'subtitles' / os.path.basename(self.subtitle_object['link_yd']))
            sub_events = parse_srt(sub_path)

            FONT_PATH = str(BASE_DIR / 'assets' / 'RobotoSerif-Medium.ttf')
            FONT_SIZE = 37
            MAX_WIDTH = final_video.w - 80
            WORDS_PER_CLIP = 4  # регулируй по вкусу

            text_clips = []

            # Группируем события
            for i in range(0, len(sub_events), WORDS_PER_CLIP):
                group = sub_events[i:i + WORDS_PER_CLIP]
                # Склеиваем слова в одну строку
                combined_text = ' '.join(ev['text'] for ev in group)
                wrapped = pro_pixel_wrap(combined_text, FONT_PATH, FONT_SIZE, MAX_WIDTH)

                txt_clip = create_centered_textclip(wrapped, FONT_PATH, FONT_SIZE, MAX_WIDTH)
                txt_clip = txt_clip.with_position(('center', 'bottom'))
                txt_clip = txt_clip.with_effects([
                    Margin(left=40, right=40, bottom=80, opacity=0)
                ])

                # Время: начало первого слова, конец последнего
                start_time = group[0]['start']
                end_time = group[-1]['end']
                txt_clip = txt_clip.with_start(start_time).with_duration(end_time - start_time)

                text_clips.append(txt_clip)

            # Градиент
            gradient = (
                ImageClip(str(BASE_DIR / "assets" / "gradient_bottom.png"))
                .with_position(("center", "bottom"))
                .with_duration(final_video.duration)
            )

            # Порядок: видео → градиент → текстовые клипы
            final_video = CompositeVideoClip([final_video, gradient] + text_clips)

            # Сохраняем результат
            final_video.write_videofile(
                str(BASE_DIR / 'temp' / 'video_tasks' / f"{self.video_task_id}_{self.type}.mp4"),
                codec="libx264", audio_codec="aac")

        finally:
            # Закрываем клипы
            for clip in clips: clip.close()
            if music: music.close()
            if voiceover: voiceover.close()
            if final_video: final_video.close()
            if subs_clip: subs_clip.close()

    def make_video_type_3(self):
        # загружаем видео и музыку
        clips = []
        music = None
        final_video = None

        try:
            for video_object in self.video_objects:
                clips.append(VideoFileClip(str(BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd']))))
            music = AudioFileClip(
                str(BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])))
            # балансируем громкость
            music = set_rms(music, 0.2)

            # определяем количество дублирования видео
            video_num = ceil((self.duration_s / UNIT_DURATION['video']) / 2)

            # склеиваем
            final_video = concatenate_videoclips([clips[0], clips[0].with_effects([TimeMirror()])] * video_num)

            # обрезаем видео под длительность
            final_video = final_video.subclipped(0, self.duration_s)

            # Если музыка длиннее итогового видео — обрезаем
            if music.duration > final_video.duration:
                music = music.subclipped(0, final_video.duration)

            # делаем затухание музыки
            music = music.with_effects([AudioFadeOut(1)])

            # Заменяем оригинальную аудиодорожку видео на музыку
            final_video = final_video.with_audio(music)

            # делаем монтаж фразы

            font_size = 35
            font_path = str(BASE_DIR / 'assets' / 'RobotoSerif-Medium.ttf')
            text = self.mistery_question
            max_width = final_video.w - 80

            wrapped_text = pro_pixel_wrap(
                text=text,
                font_path=font_path,
                font_size=font_size,
                max_width=max_width
            )

            video_text = (
                create_centered_textclip(
                    text=wrapped_text,
                    font_path=font_path,
                    font_size=font_size,
                    width=max_width
                )
                .with_effects([
                    Margin(
                        left=40,
                        right=40,
                        bottom=80,
                        opacity=0  # прозрачный фон
                    )
                ])
                .with_position(('center', 'bottom'))
                .with_duration(self.duration_s)
                .with_start(0)
            )
            # Градиентная плашка из PNG
            gradient = (
                ImageClip(str(BASE_DIR / "assets" / "gradient_bottom.png"))
                .with_position(("center", "bottom"))  # прижать к низу
                .with_duration(final_video.duration)  # растянуть на всё видео
            )

            final_video = CompositeVideoClip([final_video, gradient, video_text])

            # делаем затухание видео
            final_video = final_video.with_effects([FadeOut(1)])

            # Сохраняем результат
            final_video.write_videofile(
                str(BASE_DIR / 'temp' / 'video_tasks' / f"{self.video_task_id}_{self.type}.mp4"),
                codec="libx264", audio_codec="aac")

        finally:
            # Закрываем все клипы, чтобы освободить файлы
            for clip in clips:
                clip.close()
            if music is not None:
                music.close()
            if final_video is not None:
                final_video.close()

    def save_video(self):
        main_db.y.upload(
            str(BASE_DIR / 'temp' / 'video_tasks' / f"{self.video_task_id}_{self.type}.mp4"),
            f'app:/video_tasks/{self.video_task_id}_{self.type}.mp4')

    def make_video(self):
        # ищем ресурсы
        self.find_resources()
        # скачиваем ресурсы
        self.download_resources()
        # делаем видео
        if self.type == 1:
            self.make_video_type_1()
        if self.type == 2:
            self.make_video_type_2()
        if self.type == 3:
            self.make_video_type_3()
        # сохраняем видео на яндекс диск
        self.save_video()

        # удаляем временные файлы
        self.clear_resources()

class Carousel_task:
    def __init__(self, type, num_of_pic):
        self.type = type
        self.title = TYPES_OF_CAROUSEL_CONTENT[type]
        self.num_of_pic = num_of_pic
        self.carousel_path_list = []

    def get_task_id(self):
        if self.type == 1:
            carousel_task_dict = {
                'type': 1,
                'title': self.title,
                'pic_num': self.num_of_pic,
                'music_id': self.music_object['id'],
                'voiceover_id': None,
                'pic_list': [image_object['id'] for image_object in self.image_objects],
                'link_yd_list': None
            }
        self.carousel_task_id = main_db.write_carousel_task(carousel_task_dict)
        return self.carousel_task_id

    def find_resources(self):
        if self.type == 1:
            self.image_objects = main_db.find_images(self.num_of_pic)
            self.music_object = main_db.find_music()
            print(self.image_objects)
            print(self.music_object)

        self.get_task_id()

    def download_resources(self):
        for image_object in self.image_objects:
            main_db.y.download(image_object['link_yd'], str(BASE_DIR / 'temp' / 'images' / os.path.basename(image_object['link_yd'])))

        main_db.y.download(self.music_object['link_yd'], str(BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])))

    def clear_resources(self):
        # Удаляем видеофайлы
        for image_object in self.image_objects:
            image_path = BASE_DIR / 'temp' / 'images' / os.path.basename(image_object['link_yd'])
            if image_path.exists():
                os.remove(image_path)
        # Удаляем музыку
        music_path = BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])
        if music_path.exists():
            os.remove(music_path)
        # удаляем эти картинки
        for carousel_image in self.carousel_path_list:
            os.remove(carousel_image)

    def make_carousel_type_1(self):
        for i, image_object in enumerate(self.image_objects):
            num = i + 1
            image_path = str(BASE_DIR / 'temp' / 'images' / os.path.basename(image_object['link_yd']))
            image = Image.open(image_path)
            carousel_image_path = str(BASE_DIR / 'temp' / 'carousel_tasks' / f'{self.carousel_task_id}_{self.type}_{num}.png')
            image.save(carousel_image_path)
            self.carousel_path_list.append(carousel_image_path)
        del image

    def save_carousel(self):
        for carousel_image in self.carousel_path_list:
            main_db.y.upload(
                carousel_image,
                f'app:/carousel_tasks/{os.path.basename(carousel_image)}')

        main_db.write_carousel_task({
            'link_yd_list': [f'app:/carousel_tasks/{os.path.basename(path)}' for path in self.carousel_path_list],
        },
        self.carousel_task_id)


    def make_carousel(self):
        # ищем ресурсы
        self.find_resources()
        # скачиваем ресурсы
        self.download_resources()
        # делаем карусель
        if self.type == 1:
            self.make_carousel_type_1()
        # сохраняем видео на яндекс диск
        self.save_carousel()
        # удаляем временные файлы
        self.clear_resources()

def set_rms(clip, target_rms=0.1):
    """
    Приводит аудиоклип к заданному среднеквадратичному уровню.
    target_rms: 0.1 (~ -20 dB) — комфортный фон, 0.2 — погромче.
    """
    arr = clip.to_soundarray()
    current_rms = np.sqrt(np.mean(arr ** 2))
    if current_rms == 0:
        return clip
    factor = target_rms / current_rms
    return clip.with_volume_scaled(factor)

def create_centered_textclip(text, font_path, font_size, width, color="white"):
    font = ImageFont.truetype(font_path, font_size)

    lines = text.split("\n")

    # считаем высоту
    line_height = 40
    height = line_height * len(lines)

    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    y = 0
    for line in lines:
        w = font.getlength(line)
        x = (width - w) / 2  # 🔥 ЦЕНТРИРОВАНИЕ

        draw.text((x, y), line, font=font, fill=color)
        y += line_height

    return ImageClip(np.array(img))

def pro_pixel_wrap(text: str, font_path: str, font_size: int, max_width: int,):
    font = ImageFont.truetype(font_path, font_size)

    words = text.split()
    lines = []
    current_line = ""

    for word in words:
        test_line = word if not current_line else f"{current_line} {word}"

        if font.getlength(test_line) <= max_width:
            current_line = test_line
        else:
            lines.append(current_line)
            current_line = word

    if current_line:
        lines.append(current_line)

    return "\n".join(lines)

def parse_srt(srt_path):
    """Превращает SRT-файл в список словарей {start, end, text}."""
    events = []
    with open(srt_path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
    if not content:
        return events
    blocks = content.split('\n\n')
    for block in blocks:
        lines = block.split('\n')
        if len(lines) < 3:
            continue
        # строка времени: 00:00:01,234 --> 00:00:02,345
        time_line = lines[1]
        start_str, end_str = time_line.split(' --> ')
        def to_seconds(t):
            # t = '00:00:01,234'
            h, m, s = t.split(':')
            s, ms = s.split(',')
            return int(h)*3600 + int(m)*60 + int(s) + int(ms)/1000
        start = to_seconds(start_str)
        end = to_seconds(end_str)
        text = '\n'.join(lines[2:]).strip()
        events.append({'start': start, 'end': end, 'text': text})
    return events

if __name__ == '__main__':
    # база данных
    main_db = Main_DB(db_name, yd_token)

    # for i in range(1):
    #     # создаем задание
    #     video_task_1 = Video_task(1, 15 + round(r.uniform(0.00, 10.00), 2))
    #     print(video_task_1.title, video_task_1.num_of_videos, video_task_1.duration_s)
    #
    #     # делаем монтаж и сохраняем на яндекс диск
    #     video_task_1.make_video()

    # for i in range(3):
    #     video_task_3 = Video_task(3, 5 + round(r.uniform(0.00, 10.00), 2))
    #     print(video_task_3.title, video_task_3.num_of_videos, video_task_3.duration_s)
    #
    #     video_task_3.make_video()

    # for i in range(1):
    #     image_task_1 = Carousel_task(1, 4)
    #     print(image_task_1.title, image_task_1.num_of_pic)
    #     image_task_1.make_carousel()
    #
    # carousel_task_1 = Carousel_task(1, 4)
    # print(carousel_task_1.title, carousel_task_1.num_of_pic)
    # carousel_task_1.find_resources()

    for i in range(1):
        video_task_2 = Video_task(2, 16)
        video_task_2.make_video()
