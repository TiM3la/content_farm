import subprocess
import sys
import os
import time

# ================= LOG =================
def log(msg):
    print(f"[DEPTHFLOW] {msg}", flush=True)

# ================= SETUP =================
def initial_setup():
    log("Step 1: Installing dependencies...")

    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "depthflow",
        "yadisk",
        "gradio"
    ])

    log("Step 1: dependencies installed")

def stop_server():
    # 1️⃣ Вернуть ответ клиенту
    def delayed_exit():
        time.sleep(1.0)
        print("[DEPTHFLOW] exiting process")
        os._exit(0)

    import threading
    threading.Thread(target=delayed_exit, daemon=True).start()

    return "OK"

# ================= MAIN =================
if __name__ == "__main__":
    initial_setup()

    import yadisk
    import gradio as gr

    # -------- CONFIG --------
    TOKEN = "y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg"
    y_disk = yadisk.YaDisk(token=TOKEN)

    # ================= GENERATION =================
    def generate_video(id, img_id, img_link_yd):
        try:
            import os
            import time
            import random
            import yadisk
            from depthflow.scene import DepthScene, DepthState

            # ---------- SETTINGS ----------
            WIDTH = 768
            HEIGHT = 1344
            FPS = 30
            DURATION = 3

            EFFECTS = {
                "zoom": {
                    "method": "zoom",
                    "params": {"intensity": 1.2, "loop": False, "easing": "linear"}
                },
                "dolly": {
                    "method": "dolly",
                    "params": {"intensity": 1.2, "loop": False, "easing": "linear"}
                }
            }

            y_disk.download(img_link_yd, f'{img_id}.png')

            if id % 2 == 0:
                effect = 'zoom'
            else:
                effect = "dolly"

            scene = DepthScene(backend="headless")
            scene.ffmpeg.h264(
                preset="ultrafast",
                crf=18,
                tune="zerolatency",
                movflags="+faststart"
            )

            scene.input(image=f'{img_id}.png')

            effect_config = EFFECTS[effect]
            method_name = effect_config["method"]
            params = effect_config["params"]
            getattr(scene, method_name)(**params)

            scene.main(
                output=f'{id}.mp4',
                time=DURATION,
                fps=FPS,
                width=WIDTH,
                height=HEIGHT,
                ssaa=1.0
            )

            print(f"save: f'{id}.mp4'")

            y_disk.upload(f'{id}.mp4', f'app:/videos/{id}_{img_id}.mp4')

            os.remove(f'{img_id}.png')
            os.remove(f'{id}.mp4')
            output = f'app:/videos/{id}_{img_id}.mp4'
            return f'app:/videos/{id}_{img_id}.mp4'
        except Exception as e:
            return f"ERROR: {e}"

    # ================= GRADIO =================
    with gr.Blocks() as demo:
        gr.Markdown("## Video Generator")

        id = gr.Number(label="ID")
        img_id =  gr.Number(label="Image ID")
        img_link_yd = gr.Textbox(label="link_yd")

        generate_btn = gr.Button("Generate")
        stop_btn = gr.Button("🛑 Stop server")

        generate_btn.click(
            fn=generate_video,
            inputs=[id, img_id, img_link_yd],
            outputs=output,
            api_name="gen"
        )

        stop_btn.click(
            fn=stop_server,
            inputs=[],
            outputs=gr.Textbox(visible=False),
            api_name="stop_server"
        )

    log("Launching Gradio...")
    demo.launch(share=True, inline=False, prevent_thread_lock=True, show_error=True)

    # ================= SAVE URL =================
    time.sleep(5)

    if demo.share_url:
        with open("gradio_url_video.txt", "w") as f:
            f.write(demo.share_url)

        if not y_disk.exists("app:/gradio_urls"):
            y_disk.mkdir("app:/gradio_urls")

        y_disk.upload(
            "gradio_url_video.txt",
            "app:/gradio_urls/gradio_url_video.txt",
            overwrite=True
        )

    log("🚀 SDXL READY")

    for i in range(20):
        time.sleep(30)