import requests
import json

betting_pros_api = "https://api.bettingpros.com/v3/props?limit=25&sport=NBA&market_id=&event_id=&location=ALL&sort=bet_rating&include_events=true&include_selections=false&include_markets=true&include_books=true"

data_json = requests.get(betting_pros_api)

data = data_json.json()

try:
    with open("betting_props_api.json", 'w') as file:
        json.dump(data, file, indent=4)
except FileExistsError:
    print("File already exists.")
    
