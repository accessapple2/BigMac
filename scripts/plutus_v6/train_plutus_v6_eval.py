import os, json, torch
os.environ["HF_DATASETS_DISABLE_CACHING"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

# EVAL variant: trains on the TRAIN SPLIT ONLY (val+test held out) so the
# 178-row test set is a clean held-out eval. Registered full-data plutus-v6
# is NOT touched. Same recipe as train_plutus_v6.py (rank 8, 3 epochs).
CORPUS  = "/home/bigmac/plutus_corpus_v6.train.jsonl"   # 1,415 rows
CHATTMP = "/home/bigmac/plutus_v6_eval_chat.jsonl"
BASE    = "unsloth/Qwen2.5-7B-Instruct"
RANK    = 8
ALPHA   = 8
OUT_LORA = "/home/bigmac/plutus-v6-eval-lora"
OUT_CKPT = "/home/bigmac/plutus-v6-eval-checkpoints"

print(f"=== train_plutus_v6_EVAL | train-split only | rank={RANK} alpha={ALPHA} ===")

with open(CORPUS) as f:
    rows = [json.loads(l) for l in f if l.strip()]
with open(CHATTMP, "w") as f:
    for r in rows:
        text = f"<|im_start|>user\n{r['prompt']}<|im_end|>\n<|im_start|>assistant\n{r['completion']}<|im_end|>"
        f.write(json.dumps({"text": text}) + "\n")
print(f"Wrote {len(rows)} train-split examples to {CHATTMP}")

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
dataset = load_dataset("json", data_files=CHATTMP, split="train")
print(f"Loaded {len(dataset)} examples via load_dataset")

from trl import SFTTrainer
from transformers import TrainingArguments
trainer = SFTTrainer(
    model=model, tokenizer=tokenizer,
    train_dataset=dataset, dataset_text_field="text",
    max_seq_length=1024, dataset_num_proc=1,
    args=TrainingArguments(
        per_device_train_batch_size=4, gradient_accumulation_steps=4,
        warmup_steps=10, num_train_epochs=3, learning_rate=2e-4,
        fp16=not torch.cuda.is_bf16_supported(), bf16=torch.cuda.is_bf16_supported(),
        logging_steps=10, output_dir=OUT_CKPT, save_strategy="epoch",
        report_to="none", dataloader_num_workers=0,
    ),
)
print("Plutus v6-EVAL training on RTX 5080 (train-split only)...")
result = trainer.train()
model.save_pretrained(OUT_LORA)
tokenizer.save_pretrained(OUT_LORA)
print(f"LoRA saved to {OUT_LORA}")
losses = [h["loss"] for h in trainer.state.log_history if "loss" in h]
print(f"LOSS_CURVE: {losses}")
print(f"FINAL_STEP_LOSS: {losses[-1] if losses else 'NA'}")
try:
    print(f"MEAN_TRAIN_LOSS: {result.training_loss}")
except Exception:
    pass
print("DONE_V6_EVAL")
