from math import ceil
from generation_lib import *
from moviepy import VideoFileClip, AudioFileClip, concatenate_videoclips
import random as r


TYPES_OF_VIDEO_CONTENT = {
    1: "Склейка клипов под dark fantasy музыку",
    2: "Склейка клипов под dark fantasy музыку с озвучкой",
}

TYPES_OF_CAROUSEL_CONTENT = {
    1: "Слайд-шоу фото под dark fantasy музыку",
    2: "",
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

        if self.type == 2:
            self.voiceover_words = int(self.duration_s / UNIT_DURATION['voiceover_word'])



    def find_resources(self):
        if self.type == 1:
            self.video_objects = main_db.find_clips(self.num_of_videos)
            self.music_object = main_db.find_music()
            print(self.video_objects)
            print(self.music_object)

        self.get_task_id()

    def download_resources(self):
        for video_object in self.video_objects:
            main_db.y.download(video_object['link_yd'], str(BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd'])))

        main_db.y.download(self.music_object['link_yd'], str(BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])))

    def clear_resources(self):
        # Удаляем видеофайлы
        for video_object in self.video_objects:
            video_path = BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd'])
            if video_path.exists():
                os.remove(video_path)
        # Удаляем музыку
        music_path = BASE_DIR / 'temp' / 'music' / os.path.basename(self.music_object['link_yd'])
        if music_path.exists():
            os.remove(music_path)
        video_task_path = BASE_DIR / 'temp' / 'video_tasks' / f'{self.video_task_id}_{self.type}.mp4')
        if video_task_path.exists():
            os.remove(video_task_path)

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
        self.video_task_id = main_db.write_video_task(video_task_dict)
        return self.video_task_id

class Carousel_task:
    def __init__(self, type, num_of_pic):
        self.type = type
        self.title = TYPES_OF_CAROUSEL_CONTENT[type]
        self.num_of_pic = num_of_pic

    def find_resources(self):
        if self.type == 1:
            self.image_objects = main_db.find_images(self.num_of_pic)
            self.music_object = main_db.find_music()
            print(self.image_objects)
            print(self.music_object)

def make_video_type_1(video_task: Video_task):
    # загружаем видео и музыку
    clips = []
    music = None
    final_video = None

    try:
        for video_object in video_task.video_objects:
            clips.append(VideoFileClip(str(BASE_DIR / 'temp' / 'videos' / os.path.basename(video_object['link_yd']))))
        music = AudioFileClip(str(BASE_DIR / 'temp' / 'music' / os.path.basename(video_task.music_object['link_yd'])))

        # склеиваем
        final_video = concatenate_videoclips(clips)

        # обрезаем видео под длительность
        final_video.subclipped(0, video_task.duration_s)

        # Если музыка длиннее итогового видео — обрезаем
        if music.duration > final_video.duration:
            music = music.subclipped(0, final_video.duration)

        # Заменяем оригинальную аудиодорожку видео на музыку
        final_video = final_video.with_audio(music)

        # Сохраняем результат
        final_video.write_videofile(str(BASE_DIR / 'temp' / 'video_tasks' / f"{video_task.video_task_id}_{video_task.type}.mp4"), codec="libx264", audio_codec="aac")
        main_db.y.upload(str(BASE_DIR / 'temp' / 'video_tasks' / f"{video_task.video_task_id}_{video_task.type}.mp4"), f'app:/video_tasks/{video_task.video_task_id}_{video_task.type}.mp4')

    finally:
        # Закрываем все клипы, чтобы освободить файлы
        for clip in clips:
            clip.close()
        if music is not None:
            music.close()
        if final_video is not None:
            final_video.close()

if __name__ == '__main__':
    # база данных
    main_db = Main_DB(db_name, yd_token)

    for i in range(3):
        # создаем задание
        video_task_1 = Video_task(1, 15 + round(r.uniform(0.00, 10.00), 2))
        print(video_task_1.title, video_task_1.num_of_videos, video_task_1.duration_s)

        # ищем ресурсы
        video_task_1.find_resources()

        # скачиваем ресурсы
        video_task_1.download_resources()

        # делаем монтаж
        make_video_type_1(video_task_1)

        video_task_1.clear_resources()

    # video_task_2 = Video_task(2, 20)
    # print(video_task_2.title, video_task_2.num_of_videos, video_task_2.voiceover_words)
    #
    # carousel_task_1 = Carousel_task(1, 4)
    # print(carousel_task_1.title, carousel_task_1.num_of_pic)
    # carousel_task_1.find_resources()