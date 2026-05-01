import json

def load_prompt(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_prompt(config, user_input):
    template = config["template"]
    return template.replace("{input}", user_input)