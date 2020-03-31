import json

filename = "iso_code.json"
with open(filename) as f:
    data = json.load(f)

def get_name(lang) :
    return (data[lang]["name"])
