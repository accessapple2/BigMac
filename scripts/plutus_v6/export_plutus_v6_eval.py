import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
from unsloth import FastLanguageModel

SRC = "/home/bigmac/plutus-v6-eval-lora"
OUT = "/home/bigmac/plutus-v6-eval-gguf"

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=SRC, max_seq_length=1024, load_in_4bit=True,
)
print(f"Loaded base + LoRA from {SRC}")
model.save_pretrained_gguf(OUT, tokenizer, quantization_method="q4_k_m")
print(f"GGUF export complete: {OUT}/unsloth.Q4_K_M.gguf")
