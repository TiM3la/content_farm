# 1. Системные зависимости для работы со звуком
!apt-get update && apt-get install -y sox libsox-fmt-all -q

# 2. Основная библиотека и зависимости для инференса
!pip install qwen-tts soundfile accelerate -q

# 3. (Опционально) Обновляем transformers, если Kaggle выдает старый
!pip install "transformers>=4.48.0" -q

import torch
import soundfile as sf
from qwen_tts import Qwen3TTSModel

# Настройки
MODEL_ID = "Qwen/Qwen3-TTS-12Hz-1.7B-Base"
MY_WAV = "/kaggle/input/datasets/tim3la/qenat-voice-3/qenat_voice_3.wav"
# Текст, который РЕАЛЬНО звучит в твоем файле-образце
REF_TEXT = "Сегодня на улице стоит прекрасная погода. Я занимаюсь настройкой нейронных сетей для автоматической озвучки текстов. Это очень интересный процесс, который требует внимания к деталям и правильной настройки всех параметров модели."
# Текст, который мы ХОТИМ получить
GEN_TEXT = "Счастье — вещь нелёгкая. Его очень трудно найти внутри себя и невозможно найти где-либо в ином месте."

# 1. Загрузка модели (Без Flash Attention для стабильности)
model = Qwen3TTSModel.from_pretrained(
    MODEL_ID,
    device_map="cuda",
    dtype=torch.bfloat16
)

# 2. Генерация
print("🎤 Клонируем голос...")
wavs, sr = model.generate_voice_clone(
    text=GEN_TEXT,
    language="Russian",
    ref_audio=MY_WAV,
    ref_text=REF_TEXT
)

# 3. Сохранение
output_path = "/kaggle/working/qwen3_final_prod.wav"
sf.write(output_path, wavs[0], sr)
print(f"✨ ГОТОВО! Файл: {output_path}")