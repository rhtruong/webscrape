import requests
import json
from datetime import datetime 
import pandas as pd

BETTING_PROS_API = "https://api.bettingpros.com/v3/props?limit=25&sport=NBA&market_id=&event_id=&location=ALL&sort=bet_rating&include_events=true&include_selections=false&include_markets=true&include_books=true"

def get_bettingpros_data():
    data_json = requests.get(BETTINGPROS_API)
    data = data_json.json()

    return data
"""
Testing json

try: with open("betting_props_api.json", 'w') as file: 
    json.dump(data, file, indent=4) 
except FileExistsError: 
    print("File already exists.")
""" 

def filter_line_scores(data):    
    filtered_props = []
    
    for prop in data.get('props', []):
        try:
            prop_data = {
                'timestamp': datetime.now().isoformat(),
                'prop_id': prop.get('id'),
                'player_name': prop.get('player_name'),
                'team': prop.get('team'),
                'line': prop.get('line'),
                'over_odds': prop.get('over_odds'),
                'under_odds': prop.get('under_odds'),
            }
            
            # Add sportsbook odds if available
            if 'books' in prop and prop['books']:
                for book in prop['books'][:3]:  # Get top 3 books
                    book_name = book.get('name', 'unknown')
                    prop_data[f'{book_name}_line'] = book.get('line')
                    prop_data[f'{book_name}_over'] = book.get('over_odds')
                    prop_data[f'{book_name}_under'] = book.get('under_odds')
            
            filtered_props.append(prop_data)
        
        except Exception as e:
            print(f"Error processing prop: {e}")
            continue
    
    return filtered_props

def save_to_csv(data, outdir="data"):
    if not data:
        print("No data to save")
        return None
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename with today's date
    today = datetime.now().strftime('%Y_%m_%d')
    filename = f"nba_bettingpros_{today}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Convert to DataFrame and save
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    
    print(f"Data saved to {filepath}")
    return filepath

def main():
    #simple workflow/pipeline
    json_data = fetch_bettingpros_data()
    filtered_data = filter_line_scores(json_data)
    filepath = save_to_csv(filtered_data)
    

if __name__ == "__main__":
    main()