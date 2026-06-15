"""HM-PLUTUS-V7B-TRAIN — same corpus + rank8/alpha8/lr2e-4 as v7-3ep; change ONE variable: STOP BEFORE
MEMORIZATION. Frequent step-eval + load_best_model_at_end(val loss) + EarlyStopping; 2-epoch ceiling.
Loads the val-min checkpoint, not the 0.0036 memorized end.
"""
import os, json, torch
os.environ["HF_DATASETS_DISABLE_CACHING"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

TRAIN = "/home/bigmac/plutus_corpus_v7.train.jsonl"
VAL   = "/home/bigmac/plutus_corpus_v7.val.jsonl"
TR_CHAT = "/home/bigmac/plutus_v7_train_chat.jsonl"
VA_CHAT = "/home/bigmac/plutus_v7_val_chat.jsonl"
BASE  = "unsloth/Qwen2.5-7B-Instruct"
RANK, ALPHA = 8, 8
OUT_LORA = "/home/bigmac/plutus-v7b-lora"
OUT_CKPT = "/home/bigmac/plutus-v7b-checkpoints"
EVAL_STEPS = 12

print(f"=== train_plutus_v7b | rank={RANK} alpha={ALPHA} lr2e-4 | early-stop, eval_steps={EVAL_STEPS} ===")

def to_chat(src, dst):
    rows = [json.loads(l) for l in open(src) if l.strip()]
    with open(dst, "w") as f:
        for r in rows:
            text = f"<|im_start|>user\n{r['prompt']}<|im_end|>\n<|im_start|>assistant\n{r['completion']}<|im_end|>"
            f.write(json.dumps({"text": text}) + "\n")
    return len(rows)
print("train=%d val=%d" % (to_chat(TRAIN, TR_CHAT), to_chat(VAL, VA_CHAT)))

from unsloth import FastLanguageModel
model, tokenizer = FastLanguageModel.from_pretrained(model_name=BASE, max_seq_length=1024, load_in_4bit=True)
model = FastLanguageModel.get_peft_model(
    model, r=RANK,
    target_modules=["q_proj","k_proj","v_proj","o_proj","gate_proj","up_proj","down_proj"],
    lora_alpha=ALPHA, lora_dropout=0, bias="none", use_gradient_checkpointing="unsloth",
)

from datasets import load_dataset
train_ds = load_dataset("json", data_files=TR_CHAT, split="train")
val_ds   = load_dataset("json", data_files=VA_CHAT, split="train")

from trl import SFTTrainer
from transformers import TrainingArguments, EarlyStoppingCallback
trainer = SFTTrainer(
    model=model, tokenizer=tokenizer,
    train_dataset=train_ds, eval_dataset=val_ds, dataset_text_field="text",
    max_seq_length=1024, dataset_num_proc=1,
    callbacks=[EarlyStoppingCallback(early_stopping_patience=3)],
    args=TrainingArguments(
        per_device_train_batch_size=4, gradient_accumulation_steps=4,
        warmup_steps=10, num_train_epochs=2, learning_rate=2e-4,
        fp16=not torch.cuda.is_bf16_supported(), bf16=torch.cuda.is_bf16_supported(),
        logging_steps=EVAL_STEPS, output_dir=OUT_CKPT,
        eval_strategy="steps", eval_steps=EVAL_STEPS,
        save_strategy="steps", save_steps=EVAL_STEPS, save_total_limit=4,
        load_best_model_at_end=True, metric_for_best_model="eval_loss", greater_is_better=False,
        per_device_eval_batch_size=4, report_to="none", dataloader_num_workers=0,
    ),
)
print("Plutus v7b training (early-stop) on RTX 5080...")
result = trainer.train()
model.save_pretrained(OUT_LORA); tokenizer.save_pretrained(OUT_LORA)
print(f"BEST_CKPT: {trainer.state.best_model_checkpoint}")
print(f"BEST_VAL_LOSS: {trainer.state.best_metric}")
evals = [(h.get("step"), h.get("loss"), h.get("eval_loss")) for h in trainer.state.log_history if "eval_loss" in h or "loss" in h]
print(f"CURVE_STEP_TRAIN_VAL: {[(h.get('step'), h.get('eval_loss')) for h in trainer.state.log_history if 'eval_loss' in h]}")
print(f"TRAIN_CURVE: {[(h.get('step'), h.get('loss')) for h in trainer.state.log_history if 'loss' in h]}")
print("DONE_V7B")
