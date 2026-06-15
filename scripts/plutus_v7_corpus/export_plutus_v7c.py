"""HM-PLUTUS-V7-TRAIN — merge LoRA into base and save a 16-bit HF dir for GGUF conversion.
Uses the manual llama.cpp path downstream (unsloth save_pretrained_gguf shells `python`, absent on .168)."""
import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
from unsloth import FastLanguageModel
SRC = "/home/bigmac/plutus-v7c-lora"
OUT = "/home/bigmac/plutus-v7c-merged"
model, tokenizer = FastLanguageModel.from_pretrained(model_name=SRC, max_seq_length=1024, load_in_4bit=True)
model.save_pretrained_merged(OUT, tokenizer, save_method="merged_16bit")
print(f"MERGED_16BIT_DONE: {OUT}")
