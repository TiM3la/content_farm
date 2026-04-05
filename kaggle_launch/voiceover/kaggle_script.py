import sys, os, subprocess, time


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

    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "torch==2.4.0+cu121",
        "torchvision==0.19.0+cu121",
        "torchaudio==2.4.0+cu121",
        "--extra-index-url", "https://download.pytorch.org/whl/cu121",
        "-q"
    ])

    # 4. Остальные библиотеки
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "transformers>=4.48.0",
        "gradio",
        "yadisk",
        "qwen-tts",
        "accelerate",
        "soundfile",
        "-q"
    ])
    log("Step 1: Done. Environment ready.")



if __name__ == '__main__':

    initial_setup()


    import torch
    import gradio as gr
    import yadisk
    import soundfile as sf
    from qwen_tts import Qwen3TTSModel

    TOKEN = "y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg"
    MODEL_PATH = "/kaggle/input/datasets/tim3la/qwen3-base-model/qwen3_base_model"
    REF_WAV = "/kaggle/input/datasets/tim3la/qenat-voice-3/qenat_voice_3.wav"

    log("Step 2: Loading model...")

    model = Qwen3TTSModel.from_pretrained(
        MODEL_PATH,
        device_map="cuda",
        dtype=torch.float16,
        local_files_only=True
    )
    y_disk = yadisk.YaDisk(token=TOKEN)


    def tts_process(text_to_speak, voiceover_id, quote_id, voiceover_type, REF_TEXT):
        log(f"🎤 make: {quote_id}")
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
            if not y_disk.exists("app:/voiceovers"): y_disk.mkdir("app:/voiceovers")
            y_disk.upload(local_file, remote_path, overwrite=True)
            os.remove(local_file)
            return remote_path
        except Exception as e:
            log(f"Error: {e}")
            return str(e)


    log("Step 3: Launching Gradio...")
    demo = gr.Interface(
        fn=tts_process,
        inputs=[gr.Textbox(), gr.Number(), gr.Number(), gr.Number(), gr.Textbox()],
        outputs="text"
    )
    demo.launch(share=True, inline=False, prevent_thread_lock=True)

    with open("gradio_url.txt", "w") as f:
        f.write(demo.share_url)
    y_disk.upload("gradio_url.txt", "app:/gradio_url.txt", overwrite=True)

    log("🚀 System Ready")
    while True: time.sleep(30)