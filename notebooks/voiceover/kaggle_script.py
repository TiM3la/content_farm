import subprocess
import sys
import os
import time
import gc

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


    log("Purging incompatible versions...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "uninstall",
        "torch", "torchvision", "torchaudio", "transformers", "accelerate", "-y", "-q"
    ])


    log("Installing wheels from dataset...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "torch==2.4.0",
        "torchvision==0.19.0",
        "onnxruntime-gpu==1.24.4",
        "--no-index", "--find-links", "/kaggle/input/voiceover-wheels/wheels",
        "-q"
    ])

    # 4. ДОСТАВЛЯЕМ ОСТАЛЬНОЕ
    log("Installing rest of dependencies...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "torchaudio==2.4.0+cu121",
        "transformers>=4.48.0",
        "gradio", "yadisk", "qwen-tts", "accelerate", "soundfile",
        "--extra-index-url", "https://download.pytorch.org/whl/cu121",
        "-q"
    ])

    log("Step 1: Done. Environment is finally clean and ready.")

def stop_server():
    print("[SDXL] shutdown requested")

    # 1️⃣ Вернуть ответ клиенту
    def delayed_exit():
        time.sleep(1.0)
        try:
            torch.cuda.empty_cache()
            gc.collect()
        except:
            pass
        print("[SDXL] exiting process")
        os._exit(0)

    import threading
    threading.Thread(target=delayed_exit, daemon=True).start()

    return "OK"

if __name__ == '__main__':
    log("BOOT: starting notebook")
    initial_setup()


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

        import os

        print("=== FILES ===")
        try:
            datasets = os.listdir('/kaggle/input/')
            if not datasets:
                print("❌ null")
            else:
                for ds in datasets:
                    print(f"📁 /kaggle/input/{ds}")
        except Exception as e:
            print(f"error {e}")

        REF_WAV = f"/kaggle/input/voiceover-voices-1/{VOICES_DICT[voiceover_type]}"
        log(f"🎤 make: {quote_id} with voice ")
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
    with gr.Blocks() as demo:
        gr.Markdown("## 🎙 Qwen Voice Cloner")

        text = gr.Textbox(label="Text to speak")
        vid = gr.Number(label="Voiceover ID")
        qid = gr.Number(label="Quote ID")
        vtype = gr.Number(label="Voice Type ID")
        ref = gr.Textbox(label="Reference text")

        out = gr.Textbox(label="Result")

        gen = gr.Button("Generate")
        stop = gr.Button("🛑 Stop server")

        gen.click(
            fn=tts_process,
            inputs=[text, vid, qid, vtype, ref],
            outputs=out,
            api_name="gen"
        )

        stop.click(
            fn=stop_server,
            inputs=[],
            outputs=gr.Textbox(visible=False),
            api_name="stop_server"
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
    for i in range(20):
        time.sleep(30)