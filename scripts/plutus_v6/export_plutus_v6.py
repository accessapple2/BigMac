import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

# Merge base + v6 LoRA, export quantized GGUF. v1 artifacts untouched.
from unsloth import FastLanguageModel

SRC = "/home/bigmac/plutus-v6-lora"
OUT = "/home/bigmac/plutus-v6-gguf"

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=SRC,
    max_seq_length=1024,
    load_in_4bit=True,
)
print(f"Loaded base + LoRA from {SRC}")

model.save_pretrained_gguf(
    OUT,
    tokenizer,
    quantization_method="q4_k_m",
)
print(f"GGUF export complete: {OUT}/unsloth.Q4_K_M.gguf")
