# pip install -U diffusers accelerate transformers safetensors huggingface

import torch
import time
from diffusers import StableDiffusionXLPipeline

def log(msg):
    print(f"[SDXL] {msg}", flush=True)

start = time.time()

log("🚀 Запуск SDXL")

# ---------------- MODEL LOAD ----------------
model_id = "stabilityai/stable-diffusion-xl-base-1.0"

log("📦 Загружаем модель...")

pipe = StableDiffusionXLPipeline.from_pretrained(
    model_id,
    torch_dtype=torch.float16,
    use_safetensors=True,
)

# pipe.load_lora_weights(
#     "goofyai/SDXL-Lora-Collection",
#     weight_name="cyberpunk_style_xl-off.safetensors"
# )

log("✅ Модель загружена")

# ---------------- OPTIMIZATION ----------------
log("⚙️ Оптимизация памяти...")

pipe.enable_attention_slicing()
pipe.enable_vae_slicing()

# для T4 это важно
pipe.to("cuda")

log("🚀 Перенесено на GPU")

# ---------------- PROMPT ----------------
prompt = "a wizard walks through the forest to a large magical tree. the tree glows. it's night. there are castles all around. it's medieval. it's fantasy phone wallpaper, centered composition, cinematic framing"

log(f"🧠 Prompt: {prompt}")

# ---------------- GENERATION ----------------
log("🎬 Генерация...")

t0 = time.time()

image = pipe(
    prompt=prompt,
    negative_prompt="blurry, low quality, bad anatomy",
    num_inference_steps=30,
    guidance_scale=6.0,
    height=1344,
    width=768,
).images[0]

log(f"⏱ Готово за {time.time() - t0:.2f}s")

# ---------------- SAVE ----------------
image.save("result_5.png")

log(f"💾 Сохранено result_2.png")

log(f"🏁 TOTAL TIME: {time.time() - start:.2f}s")

# отличная картинка!