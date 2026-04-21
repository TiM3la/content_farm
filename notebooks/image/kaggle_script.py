import subprocess
import sys
import os
import time
import gc

# ================= LOG =================
def log(msg):
    print(f"[SDXL] {msg}", flush=True)

# ================= SETUP =================
def initial_setup():
    log("Step 1: Installing dependencies from wheels...")

    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "--no-index",
        "--find-links", "/kaggle/input/image-wheels/sdxl-wheels/wheels",

        "diffusers==0.36.0",
        "accelerate==1.12.0",
        "transformers==5.0.0",
        "safetensors==0.7.0",
        "einops==0.8.2",
        "gradio==5.50.0",
        "gradio_client==1.14.0",
        "yadisk",
    ])

    log("Step 1: dependencies installed")

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

# ================= MAIN =================
if __name__ == "__main__":
    initial_setup()

    import torch
    import gradio as gr
    import yadisk
    from diffusers import StableDiffusionXLPipeline
    from PIL import Image

    # -------- CONFIG --------
    TOKEN = "y0__xCigd-mCBj76D8gyKKS-hZpnEvyFrPRUfxqf66KxY3EqDTWOg"

    BASE_MODEL_PATH = "/kaggle/input/sdxl-model/sdxl-base-1.0"
    LORA_PATH = "/kaggle/input/my-lora-1/mylora_4_ep6.safetensors"

    y_disk = yadisk.YaDisk(token=TOKEN)

    torch.cuda.empty_cache()
    gc.collect()

    log("Loading SDXL Base model...")

    pipe = StableDiffusionXLPipeline.from_pretrained(
        BASE_MODEL_PATH,
        torch_dtype=torch.float16,
        use_safetensors=True,
    )

    log("Applying optimizations...")

    pipe.enable_model_cpu_offload()
    pipe.enable_attention_slicing()
    pipe.enable_vae_slicing()

    # MY LORA
    # pipe.load_lora_weights(
    #     "/kaggle/input/my-lora-1",
    #     weight_name="mylora_4_ep6.safetensors",
    #     adapter_name="mylora",
    #     local_files_only=True
    # )
    # LORA 1
    pipe.load_lora_weights(
        "/kaggle/input/my-lora-1",
        weight_name="lora_1.safetensors",
        adapter_name="mylora",
        local_files_only=True
    )

    log("Model ready")

    # ================= GENERATION =================
    def generate_image(id, prompt_id, prompt, lora_weight=None, seed=None):
        log(f'CALL: id={id}, prompt_id={prompt_id}, lora={lora_weight}, seed={seed}')

        try:
            remote_dir = "app:/images"
            if not y_disk.exists(remote_dir):
                y_disk.mkdir(remote_dir)
                log(f"Created remote dir: {remote_dir}")

            NEG = ("bad anatomy, extra limbs, missing arms, missing legs, "
                   "extra fingers, fused fingers, ugly face, deformed face, "
                   "blurry, low quality, worst quality")

            if lora_weight is not None:

                pipe.set_adapters(["mylora"], adapter_weights=[float(lora_weight)])

                # ---------- BASE ----------
                log("Generating base image...")
                base_image = pipe(
                    prompt=prompt,
                    negative_prompt=NEG,
                    num_inference_steps=20,
                    guidance_scale=4,
                    width=768,
                    height=1344,
                ).images[0]
                log("Base image generated successfully.")

                filename = f"{id}_{prompt_id}.png"

                try:
                    base_image.save(filename)
                    log(f"Lowq image saved locally: {filename} ({os.path.getsize(filename)} bytes)")
                except Exception as e:
                    log(f"ERROR saving lowq image: {e}")

                remote_path = f"{remote_dir}/{filename}"

                try:
                    log(f"Uploading to {remote_path}...")
                    y_disk.upload(filename, remote_path, overwrite=True)
                    log(f"Lowq uploaded successfully: {remote_path}")
                except Exception as e:
                    log(f"ERROR uploading lowq: {e}")

                if os.path.exists(filename):
                    os.remove(filename)
                    log(f"Removed local file: {filename}")

                del base_image
                torch.cuda.empty_cache()
                gc.collect()

                return remote_path
            # ================= MODE 2: SEED + WEIGHTS LOOP =================
            else:
                log("more image...")

                weights_list = [0, 1.2, 1.5, 1.7, 2.0]
                results = []

                for w in weights_list:
                    pipe.set_adapters(["mylora"], adapter_weights=[float(w)])

                    generator = torch.Generator(device="cuda").manual_seed(int(seed))

                    # ---------- BASE ----------
                    base_image = pipe(
                        prompt=prompt,
                        negative_prompt=NEG,
                        num_inference_steps=20,
                        guidance_scale=4,
                        width=512,
                        height=896,
                        generator=generator
                    ).images[0]

                    filename = f"{id}_{prompt_id}_w{w}_s{seed}.png"
                    base_image.save(filename)

                    remote_path = f"{remote_dir}/{filename}"
                    y_disk.upload(filename, remote_path, overwrite=True)
                    os.remove(filename)

                    del base_image
                    torch.cuda.empty_cache()
                    gc.collect()

                    results.append(remote_path)

                return "\n".join(results)

        except Exception as e:
            return f"ERROR: {e}"

    # ================= GRADIO =================
    with gr.Blocks() as demo:
        gr.Markdown("## SDXL Generator")

        id_in = gr.Number(label="ID")
        prompt_id_in = gr.Number(label="Prompt ID")
        prompt_in = gr.Textbox(label="Prompt")
        lora_in = gr.Number(label="LoRA weight")
        seed_in = gr.Number(label="Seed (optional)")
        output = gr.Textbox(label="Result")

        generate_btn = gr.Button("Generate")
        stop_btn = gr.Button("🛑 Stop server")

        generate_btn.click(
            fn=generate_image,
            inputs=[id_in, prompt_id_in, prompt_in, lora_in, seed_in],
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
        with open("gradio_url_image.txt", "w") as f:
            f.write(demo.share_url)

        if not y_disk.exists("app:/gradio_urls"):
            y_disk.mkdir("app:/gradio_urls")

        y_disk.upload(
            "gradio_url_image.txt",
            "app:/gradio_urls/gradio_url_image.txt",
            overwrite=True
        )

    log("🚀 SDXL READY")

    for i in range(20):
        time.sleep(30)