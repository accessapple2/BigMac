"""HM-PLUTUS-V8-TRAIN — v7d recipe (r16/a16/early-stop) on the v8 diversity corpus.

Only one variable changes from v7d: the corpus (multi-author diversity corpus replaces
the single-author Grok corpus). Recipe, base model, and all hyperparams are identical.

Run on .168 RTX 5080 box — copy corpus files first:
  scp data/plutus_v8/plutus_corpus_v8.train.jsonl bigmac@192.168.1.168:~/plutus_v8_train.jsonl
  scp data/plutus_v8/plutus_corpus_v8.val.jsonl   bigmac@192.168.1.168:~/plutus_v8_val.jsonl
  ssh bigmac@192.168.1.168 'cd ~ && .venv-train/bin/python3 ...'
"""
import os, json, torch
os.environ["HF_DATASETS_DISABLE_CACHING"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"

TRAIN = "/home/bigmac/plutus_v8_train.jsonl"
VAL   = "/home/bigmac/plutus_v8_val.jsonl"
TR_CHAT = "/home/bigmac/plutus_v8_train_chat.jsonl"
VA_CHAT = "/home/bigmac/plutus_v8_val_chat.jsonl"
BASE  = "unsloth/Qwen2.5-7B-Instruct"
RANK, ALPHA = 16, 16
OUT_LORA = "/home/bigmac/plutus-v8-lora"
OUT_CKPT = "/home/bigmac/plutus-v8-checkpoints"
EVAL_STEPS = 12

print(f"=== train_plutus_v8 | rank={RANK} alpha={ALPHA} lr2e-4 | early-stop | diversity corpus ===")


def to_chat(src, dst):
    rows = [json.loads(l) for l in open(src) if l.strip()]
    with open(dst, "w") as f:
        for r in rows:
            text = (f"<|im_start|>user\n{r['prompt']}<|im_end|>\n"
                    f"<|im_start|>assistant\n{r['completion']}<|im_end|>")
            f.write(json.dumps({"text": text}) + "\n")
    return len(rows)


print("train=%d val=%d" % (to_chat(TRAIN, TR_CHAT), to_chat(VAL, VA_CHAT)))

from unsloth import FastLanguageModel
model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=BASE, max_seq_length=1024, load_in_4bit=True)
model = FastLanguageModel.get_peft_model(
    model, r=RANK,
    target_modules=["q_proj", "k_proj", "v_proj", "o_proj",
                    "gate_proj", "up_proj", "down_proj"],
    lora_alpha=ALPHA, lora_dropout=0, bias="none",
    use_gradient_checkpointing="unsloth",
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
print("Plutus v8 training (early-stop) on RTX 5080...")
result = trainer.train()
model.save_pretrained(OUT_LORA); tokenizer.save_pretrained(OUT_LORA)
print(f"BEST_CKPT: {trainer.state.best_model_checkpoint}")
print(f"BEST_VAL_LOSS: {trainer.state.best_metric}")
print(f"CURVE_STEP_VAL: {[(h.get('step'), h.get('eval_loss')) for h in trainer.state.log_history if 'eval_loss' in h]}")
print(f"TRAIN_CURVE: {[(h.get('step'), h.get('loss')) for h in trainer.state.log_history if 'loss' in h]}")
print("DONE_V8")
