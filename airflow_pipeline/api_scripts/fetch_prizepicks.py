import requests
import json
from datetime import datetime 
import pandas as pd
import os

PRIZEPICKS_API = "https://api.prizepicks.com/projections"

def get_prizepicks_data():
    data_json = requests.get(PRIZEPICKS_API)
    data = data_json.json()

    return data

def filter_line_scores(data):    
    filtered_projections = []
    
    # PrizePicks API structure: data.projections and data.included
    projections = data.get('data', [])
    included = data.get('included', [])
    
    players = {p['id']: p for p in included if p['type'] == 'new_player'}
    leagues = {l['id']: l for l in included if l['type'] == 'league'}
    
    for proj in projections:
        try:
            # Get relationships
            relationships = proj.get('relationships', {})
            player_id = relationships.get('new_player', {}).get('data', {}).get('id')
            
            
            # Get player info
            player = players.get(player_id, {})
            player_attrs = player.get('attributes', {})
            
            # Get projection attributes
            proj_attrs = proj.get('attributes', {})
            
            proj_data = {
                'timestamp': datetime.now().isoformat(),
                'projection_id': proj.get('id'),
                'player_name': player_attrs.get('name'),
                'team': player_attrs.get('team'),
                'position': player_attrs.get('position'),
                'stat_type': proj_attrs.get('stat_type'),
                'line_score': proj_attrs.get('line_score'),
                'description': proj_attrs.get('description'),
                'start_time': proj_attrs.get('start_time'),
            }
            
            filtered_projections.append(proj_data)
        
        except Exception as e:
            continue
    
    return filtered_projections

def save_to_csv(data, output_dir="data"):
    if not data:
        print("No data to save")
        return None
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename with today's date
    today = datetime.now().strftime('%Y_%m_%d')
    filename = f"nba_prizepicks_{today}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Convert to DataFrame and save
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    
    print(f"Data saved to {filepath}")
    return filepath

def main():
    #simple workflow/pipeline
    json_data = get_prizepicks_data()
    filtered_data = filter_line_scores(json_data)
    filepath = save_to_csv(filtered_data)
    
    return filepath

if __name__ == "__main__":
    main()