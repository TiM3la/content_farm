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
    print("[SDXL] shutting down gracefully...")
    try:
        demo.close()
    except:
        pass

    torch.cuda.empty_cache()
    gc.collect()

    print("[SDXL] cleanup done. exiting...")
    raise SystemExit(0)

# ================= MAIN =================
if __name__ == "__main__":
    initial_setup()

    import torch
    import gradio as gr
    import yadisk
    from diffusers import StableDiffusionXLPipeline

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
    pipe.vae.enable_slicing()

    pipe.load_lora_weights(
        "/kaggle/input/my-lora-1",
        weight_name="mylora_4_ep6.safetensors",
        adapter_name="mylora",
        local_files_only=True
    )

    log("Model ready")

    # ================= GENERATION =================
    def generate_image(id, prompt_id, prompt, lora_weight=None, seed=None):
        print(id, prompt_id, prompt, lora_weight, seed)
        try:
            log("1...")
            remote_dir = "app:/images"

            if not y_disk.exists(remote_dir):
                y_disk.mkdir(remote_dir)

            # ================= MODE 1: SINGLE IMAGE =================
            log("single_image...")
            if seed is None:
                pipe.set_adapters(["mylora"], adapter_weights=[float(lora_weight)])

                log("pipe...")
                image = pipe(
                    prompt=prompt,
                    negative_prompt="bad anatomy, extra limbs, missing arms, missing legs, extra fingers, fused fingers, ugly face, deformed face, blurry, low quality, worst quality",
                    num_inference_steps=30,
                    guidance_scale=4,
                    width=768,
                    height=1344,
                ).images[0]

                filename = f"{id}_{prompt_id}.png"

                image.save(filename)
                remote_path = f"{remote_dir}/{filename}"

                y_disk.upload(filename, remote_path, overwrite=True)
                os.remove(filename)

                del image
                torch.cuda.empty_cache()
                gc.collect()

                return remote_path

            # ================= MODE 2: SEED + WEIGHTS LOOP =================
            else:
                weights_list = [0, 1.2, 1.5, 1.7, 2.0]

                results = []

                for w in weights_list:
                    pipe.set_adapters(["mylora"], adapter_weights=[float(w)])

                    generator = torch.Generator(device="cuda").manual_seed(int(seed))

                    image = pipe(
                        prompt=prompt,
                        negative_prompt="bad anatomy, extra limbs, missing arms, missing legs, extra fingers, fused fingers, ugly face, deformed face, blurry, low quality, worst quality",
                        num_inference_steps=30,
                        guidance_scale=4,
                        width=768,
                        height=1344,
                        generator=generator
                    ).images[0]

                    filename = f"{id}_{prompt_id}_w{w}_s{seed}.png"

                    image.save(filename)
                    remote_path = f"{remote_dir}/{filename}"

                    y_disk.upload(filename, remote_path, overwrite=True)
                    os.remove(filename)

                    del image
                    torch.cuda.empty_cache()
                    gc.collect()

                    results.append(remote_path)

                return "\n".join(results)

        except Exception as e:
            return f"ERROR: {e}"

    # ================= GRADIO =================
    demo = gr.Interface(
        fn=generate_image,
        inputs=[
            gr.Number(label="ID"),
            gr.Number(label="Prompt ID"),
            gr.Textbox(label="Prompt"),
            gr.Number(label="LoRA weight (used only if seed is empty)"),
            gr.Number(label="Seed (optional batch mode)")
        ],
        outputs="text"
    )

    # gr.Interface(fn=stop_server, inputs=[], outputs=[], api_name="/stop_server")

    log("Launching Gradio...")
    demo.launch(share=True, inline=False, prevent_thread_lock=True)

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

    while True:
        time.sleep(30)