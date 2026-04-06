import subprocess
import sys
import os
import time

VOICES_DICT = {
    1: "qenat_voice_2.wav",
    2: "qenat_voice_3.wav",
}


def log(msg):
    print(f"[DEBUG] {msg}", flush=True)


def initial_setup():
    log("Step 1: Installing System and Python dependencies...")

    subprocess.check_call(['apt-get', 'update', '-q'])
    subprocess.check_call(['apt-get', 'install', '-y', 'sox', 'libsox-fmt-all', '-q'])

    subprocess.check_call([
        sys.executable, "-m", "pip", "uninstall",
        "torch", "torchvision", "torchaudio", "transformers", "accelerate", "-y"
    ])

    WHEELS_PATH = "/kaggle/input/voiceover-wheels/wheels"


    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "--no-index", "--find-links", WHEELS_PATH,
        "torch", "torchvision", "torchaudio", "transformers",
        "gradio", "yadisk", "qwen-tts", "accelerate", "soundfile",
        "onnxruntime-gpu", "flash-attn", "sox"  # добавил python-sox сюда же
    ])

    log("Step 1: Done. Environment ready.")


if __name__ == '__main__':
    initial_setup()

    # Импорты ТОЛЬКО после установки
    import torch
    import gradio as gr
    import yadisk
    import soundfile as sf
    from qwen_tts import Qwen3TTSModel

    TOKEN = "y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg"
    MODEL_PATH = "/kaggle/input/datasets/tim3la/qwen3-base-model/qwen3_base_model"

    log("Step 2: Loading model...")
    model = Qwen3TTSModel.from_pretrained(
        MODEL_PATH,
        device_map="cuda",
        dtype=torch.float16,
        local_files_only=True
    )
    y_disk = yadisk.YaDisk(token=TOKEN)


    def tts_process(text_to_speak, voiceover_id, quote_id, voiceover_type, REF_TEXT):
        # Используем глобальный VOICES_DICT
        voice_file = VOICES_DICT.get(int(voiceover_type), "qenat_voice_3.wav")
        REF_WAV = f"/kaggle/input/datasets/tim3la/voiceover-voices/{voice_file}"

        log(f"🎤 make: {quote_id} with voice {voice_file}")
        try:
            wavs, sr = model.generate_voice_clone(
                text=text_to_speak,
                language="Russian",
                ref_audio=REF_WAV,
                ref_text=REF_TEXT
            )
            local_file = f"temp_{voiceover_id}.wav"
            sf.write(local_file, wavs[0], sr)

            remote_path = f"app:/voiceovers/{voiceover_id}_{quote_id}_{voiceover_type}.wav"

            # Проверка папки на диске
            if not y_disk.exists("app:/voiceovers"):
                y_disk.mkdir("app:/voiceovers")

            y_disk.upload(local_file, remote_path, overwrite=True)
            os.remove(local_file)
            return remote_path
        except Exception as e:
            log(f"Error: {e}")
            return str(e)


    log("Step 3: Launching Gradio...")
    demo = gr.Interface(
        fn=tts_process,

        inputs=[
            gr.Textbox(label="Text to speak"),
            gr.Number(label="Voiceover ID"),
            gr.Number(label="Quote ID"),
            gr.Number(label="Voice Type (ID)"),
            gr.Textbox(label="Reference Text")
        ],
        outputs="text"
    )


    demo.launch(share=True, inline=False, prevent_thread_lock=True)


    time.sleep(5)
    if demo.share_url:
        with open("gradio_url_voiceover.txt", "w") as f:
            f.write(demo.share_url)

        # Проверяем папку для ссылок
        if not y_disk.exists("app:/gradio_urls"):
            y_disk.mkdir("app:/gradio_urls")
        y_disk.upload("gradio_url_voiceover.txt", "app:/gradio_urls/gradio_url_voiceover.txt", overwrite=True)

    log("🚀 System Ready")
    while True:
        time.sleep(30)