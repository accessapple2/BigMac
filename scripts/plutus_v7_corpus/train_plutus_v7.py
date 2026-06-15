"""HM-PLUTUS-V7-TRAIN — LoRA train v7 on the v7 corpus. SAME recipe as v6 (rank8/alpha8/3ep/lr2e-4,
Qwen2.5-7B-Instruct) — ONLY the corpus changed, so any gain is attributable to the data fix.

Runs on .168 (/home/bigmac paths) via ~/plutus-train-pinned. Adds VAL-loss eval (measurement only —
does NOT change the training recipe) for the overfit/memorization check the gate requires.
"""
import os, json, torch
os.environ["HF_DATASETS_DISABLE_CACHING"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

TRAIN = "/home/bigmac/plutus_corpus_v7.train.jsonl"   # 1,150 rows
VAL   = "/home/bigmac/plutus_corpus_v7.val.jsonl"     # 143 rows
TR_CHAT = "/home/bigmac/plutus_v7_train_chat.jsonl"
VA_CHAT = "/home/bigmac/plutus_v7_val_chat.jsonl"
BASE  = "unsloth/Qwen2.5-7B-Instruct"
RANK, ALPHA = 8, 8
OUT_LORA = "/home/bigmac/plutus-v7-lora"
OUT_CKPT = "/home/bigmac/plutus-v7-checkpoints"

print(f"=== train_plutus_v7 | rank={RANK} alpha={ALPHA} 3ep lr2e-4 | corpus-only change vs v6 ===")

def to_chat(src, dst):
    with open(src) as f:
        rows = [json.loads(l) for l in f if l.strip()]
    with open(dst, "w") as f:
        for r in rows:
            text = f"<|im_start|>user\n{r['prompt']}<|im_end|>\n<|im_start|>assistant\n{r['completion']}<|im_end|>"
            f.write(json.dumps({"text": text}) + "\n")
    return len(rows)

ntr = to_chat(TRAIN, TR_CHAT); nva = to_chat(VAL, VA_CHAT)
print(f"train={ntr} val={nva}")

from unsloth import FastLanguageModel
model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=BASE, max_seq_length=1024, load_in_4bit=True,
)
model = FastLanguageModel.get_peft_model(
    model, r=RANK,
    target_modules=["q_proj","k_proj","v_proj","o_proj","gate_proj","up_proj","down_proj"],
    lora_alpha=ALPHA, lora_dropout=0, bias="none",
    use_gradient_checkpointing="unsloth",
)

from datasets import load_dataset
train_ds = load_dataset("json", data_files=TR_CHAT, split="train")
val_ds   = load_dataset("json", data_files=VA_CHAT, split="train")

from trl import SFTTrainer
from transformers import TrainingArguments
trainer = SFTTrainer(
    model=model, tokenizer=tokenizer,
    train_dataset=train_ds, eval_dataset=val_ds, dataset_text_field="text",
    max_seq_length=1024, dataset_num_proc=1,
    args=TrainingArguments(
        per_device_train_batch_size=4, gradient_accumulation_steps=4,
        warmup_steps=10, num_train_epochs=3, learning_rate=2e-4,
        fp16=not torch.cuda.is_bf16_supported(), bf16=torch.cuda.is_bf16_supported(),
        logging_steps=10, output_dir=OUT_CKPT, save_strategy="epoch",
        eval_strategy="epoch", per_device_eval_batch_size=4,
        report_to="none", dataloader_num_workers=0,
    ),
)
print("Plutus v7 training on RTX 5080...")
result = trainer.train()
model.save_pretrained(OUT_LORA)
tokenizer.save_pretrained(OUT_LORA)
print(f"LoRA saved to {OUT_LORA}")

losses = [h["loss"] for h in trainer.state.log_history if "loss" in h]
evals  = [(h.get("epoch"), h["eval_loss"]) for h in trainer.state.log_history if "eval_loss" in h]
print(f"LOSS_CURVE: {losses}")
print(f"FINAL_STEP_LOSS: {losses[-1] if losses else 'NA'}")
print(f"EVAL_LOSS_BY_EPOCH: {evals}")
try:
    print(f"MEAN_TRAIN_LOSS: {result.training_loss}")
except Exception:
    pass
print("DONE_V7")
