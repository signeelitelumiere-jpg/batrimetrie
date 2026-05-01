#!/usr/bin/env python3
import argparse
import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig


def get_bnb_config():
    return BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_use_double_quant=True,
        bnb_4bit_compute_dtype=torch.float16,
    )


def load_model(model_path: str, device_map: str = "auto"):
    """Charge le tokenizer et le modèle quantifié en 4 bits depuis `model_path`.

    Retourne `(tokenizer, model, device)`.
    """
    bnb_config = get_bnb_config()

    tokenizer = AutoTokenizer.from_pretrained(model_path, use_fast=False)

    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        quantization_config=bnb_config,
        device_map=device_map,
        trust_remote_code=False,
        use_safetensors=True,
    )

    device = next(model.parameters()).device
    return tokenizer, model, device


def generate_text(model, tokenizer, prompt: str, max_new_tokens: int = 128, **gen_kwargs):
    device = next(model.parameters()).device
    inputs = tokenizer(prompt, return_tensors="pt").to(device)
    with torch.no_grad():
        out = model.generate(**inputs, max_new_tokens=max_new_tokens, **gen_kwargs)
    return tokenizer.decode(out[0], skip_special_tokens=True)


def main():
    p = argparse.ArgumentParser(description="Charger un modèle en 4 bits (bnb NF4) et générer un exemple")
    p.add_argument("--model-dir", required=False,
                   default=os.path.join(os.path.dirname(__file__), "..", "kibali-final-merged"),
                   help="Chemin vers le dossier du modèle (safetensors)")
    p.add_argument("--prompt", required=False, default="Bonjour, expliquez brièvement:", help="Prompt de test")
    args = p.parse_args()

    model_path = args.model_dir

    print("CUDA disponible:", torch.cuda.is_available())

    print("Chargement du tokenizer et du modèle...")
    tokenizer, model, device = load_model(model_path)
    print(f"Modèle chargé sur {device}")

    print("Génération d'un texte de test...")
    text = generate_text(model, tokenizer, args.prompt, max_new_tokens=128, do_sample=True, top_p=0.95)

    print("--- Résultat ---")
    print(text)


if __name__ == "__main__":
    main()
