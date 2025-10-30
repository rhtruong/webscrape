import json


with open("betting_pros_api.json", 'r') as file:
    betting_pros_data = json.load(file)

filtered_list = []

for data in betting_pros_data:
    
