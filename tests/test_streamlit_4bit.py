import streamlit as st
import torch
import json
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from langgraph.graph import StateGraph

MODEL_PATH = r"C:\Users\Admin\Pictures\DAT.ERT\batrimetrie\kibali-final-merged"
PROMPT_PATH = r"C:\Users\Admin\Pictures\DAT.ERT\batrimetrie\prompts\base_prompt.json"

# -------------------------
# Charger prompt JSON
# -------------------------
def load_prompt():
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def build_prompt(config, user_input):
    return config["template"].replace("{input}", user_input)

prompt_config = load_prompt()

# -------------------------
# Charger modèle
# -------------------------
@st.cache_resource
def load_model():
    bnb_config = BitsAndBytesConfig(load_in_4bit=True)

    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        quantization_config=bnb_config,
        device_map="auto"
    )

    return tokenizer, model

tokenizer, model = load_model()

# -------------------------
# Graph State
# -------------------------
class GraphState(dict):
    pass

# -------------------------
# Nodes
# -------------------------
def prepare_prompt(state):
    user_input = state["input"]
    final_prompt = build_prompt(prompt_config, user_input)

    return {"prompt": final_prompt}

def generate(state):
    prompt = state["prompt"]

    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)

    output = model.generate(
        **inputs,
        max_new_tokens=200,
        temperature=prompt_config["constraints"]["temperature"]
    )

    response = tokenizer.decode(output[0], skip_special_tokens=True)

    return {"output": response}

# -------------------------
# Graph
# -------------------------
builder = StateGraph(GraphState)

builder.add_node("prepare", prepare_prompt)
builder.add_node("generate", generate)

builder.set_entry_point("prepare")
builder.add_edge("prepare", "generate")

graph = builder.compile()

# -------------------------
# UI
# -------------------------
st.title("🧠 IA pilotée par JSON")

user_input = st.text_area("💬 Question")

if st.button("Envoyer"):
    result = graph.invoke({"input": user_input})

    st.subheader("Réponse")
    st.write(result["output"])