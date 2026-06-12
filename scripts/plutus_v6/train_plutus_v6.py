import os, json, torch, pathlib
os.environ["HF_DATASETS_DISABLE_CACHING"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

# ---- config -------------------------------------------------------------
SMOKE   = os.environ.get("SMOKE", "0") == "1"
CORPUS  = "/home/bigmac/plutus_corpus_v6.jsonl"
CHATTMP = "/home/bigmac/plutus_v6_chat.jsonl"          # v6-specific (v1 untouched)
BASE    = "unsloth/Qwen2.5-7B-Instruct"
RANK    = 8                                             # spec: rank 8 (v1 was 16)
ALPHA   = 8                                             # keep alpha==rank ratio, as v1 (16/16)

if SMOKE:
    OUT_LORA  = "/home/bigmac/plutus-v6-smoke-lora"
    OUT_CKPT  = "/home/bigmac/plutus-v6-smoke-checkpoints"
else:
    OUT_LORA  = "/home/bigmac/plutus-v6-lora"
    OUT_CKPT  = "/home/bigmac/plutus-v6-checkpoints"

print(f"=== train_plutus_v6 | SMOKE={SMOKE} | rank={RANK} alpha={ALPHA} ===")

# ---- data ---------------------------------------------------------------
with open(CORPUS) as f:
    rows = [json.loads(l) for l in f if l.strip()]

with open(CHATTMP, "w") as f:
    for r in rows:
        text = f"<|im_start|>user\n{r['prompt']}<|im_end|>\n<|im_start|>assistant\n{r['completion']}<|im_end|>"
        f.write(json.dumps({"text": text}) + "\n")
print(f"Wrote {len(rows)} examples to {CHATTMP}")

# ---- model --------------------------------------------------------------
from unsloth import FastLanguageModel

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=BASE,
    max_seq_length=1024, load_in_4bit=True,
)
model = FastLanguageModel.get_peft_model(
    model, r=RANK,
    target_modules=["q_proj","k_proj","v_proj","o_proj","gate_proj","up_proj","down_proj"],
    lora_alpha=ALPHA, lora_dropout=0, bias="none",
    use_gradient_checkpointing="unsloth",
)

from datasets import load_dataset
dataset = load_dataset("json", data_files=CHATTMP, split="train")
print(f"Loaded {len(dataset)} examples via load_dataset")

# ---- trainer ------------------------------------------------------------
from trl import SFTTrainer
from transformers import TrainingArguments

ta_kwargs = dict(
    per_device_train_batch_size=4, gradient_accumulation_steps=4,
    warmup_steps=10, learning_rate=2e-4,
    fp16=not torch.cuda.is_bf16_supported(), bf16=torch.cuda.is_bf16_supported(),
    logging_steps=1 if SMOKE else 10,
    output_dir=OUT_CKPT, report_to="none", dataloader_num_workers=0,
)
if SMOKE:
    ta_kwargs.update(max_steps=10, save_strategy="no")
else:
    ta_kwargs.update(num_train_epochs=3, save_strategy="epoch")

trainer = SFTTrainer(
    model=model, tokenizer=tokenizer,
    train_dataset=dataset, dataset_text_field="text",
    max_seq_length=1024, dataset_num_proc=1,
    args=TrainingArguments(**ta_kwargs),
)

print(f"Plutus v6 training on RTX 5080 (SMOKE={SMOKE})...")
result = trainer.train()

# ---- save ---------------------------------------------------------------
model.save_pretrained(OUT_LORA)
tokenizer.save_pretrained(OUT_LORA)
print(f"LoRA saved to {OUT_LORA}")

# Loss reporting
losses = [h["loss"] for h in trainer.state.log_history if "loss" in h]
print(f"LOSS_CURVE: {losses}")
print(f"FINAL_STEP_LOSS: {losses[-1] if losses else 'NA'}")
try:
    print(f"MEAN_TRAIN_LOSS: {result.training_loss}")
except Exception:
    pass
print("DONE_V6")
