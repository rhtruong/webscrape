import requests
import json
import pandas as pd
from datetime import datetime
import sys

DRAFTEDGE_API = "https://draftedge.com/draftedge-data/nbaprops1.json"

def fetch_draftedge_data():
    """Fetch NBA props from DraftEdge API"""
    try:
        response = requests.get(DRAFTEDGE_API, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching DraftEdge data: {e}", file=sys.stderr)
        return None

def parse_draftedge_props(data):
    """
    Parse DraftEdge API response and extract standardized prop data.
    Focuses on points props from various sportsbooks.
    
    Returns:
        list: List of dictionaries with standardized schema
    """
    if not data:
        return []
    
    parsed_props = []
    time_scraped = datetime.now().isoformat()
    
    for player_id, player_data in data.items():
        try:
            # Get basic player info
            player_name = player_data.get('Player')
            team = player_data.get('DFS_Team')
            opponent_team = player_data.get('Opp')
            game_time = player_data.get('GameTime')
            
            # Skip if missing essential info
            if not player_name or not team:
                continue
            
            # Convert game time to ISO format if possible
            game_start = None
            if game_time:
                try:
                    # GameTime format: "11-01 17:00 EDT"
                    # Convert to ISO format (approximate - uses current year)
                    current_year = datetime.now().year
                    game_datetime_str = f"{current_year}-{game_time}"
                    # Parse and convert to ISO
                    parsed_dt = datetime.strptime(game_datetime_str.split(' EDT')[0], "%Y-%m-%d %H:%M")
                    game_start = parsed_dt.isoformat()
                except Exception as e:
                    game_start = game_time
            
            # Get prop data
            prop_data = player_data.get('PropData', {})
            points_props = prop_data.get('PropPts', [])
            
            # Define sportsbooks that typically provide main lines only
            # These books usually have 1-2 lines max and don't clutter with alternates
            MAIN_LINE_BOOKS = {
                'fanduel', 'draftkings', 'betmgm', 'fanatics', 
                'williamhill_us', 'betonlineag'
            }
            
            # Track unique lines per bookmaker (to handle any duplicates)
            bookmaker_lines = {}
            
            # Process each sportsbook's points line
            for prop in points_props:
                bet_type = prop.get('BetType')
                
                # Only process "Over" lines (the line score is the same for Over/Under)
                if bet_type != 'Over':
                    continue
                
                line_score = prop.get('Line')
                bookmaker = prop.get('Bookmaker')
                
                # Skip if missing essential prop info
                if line_score is None or not bookmaker:
                    continue
                
                # FILTER: Only include main line sportsbooks
                if bookmaker.lower() not in MAIN_LINE_BOOKS:
                    continue
                
                # Track lines by bookmaker (use dict to avoid duplicates)
                if bookmaker not in bookmaker_lines:
                    bookmaker_lines[bookmaker] = line_score
            
            # Create one row per bookmaker with their line
            for bookmaker, line_score in bookmaker_lines.items():
                # Format bookmaker name (capitalize first letter)
                sportsbook = bookmaker.replace('_', ' ').title()
                
                parsed_prop = {
                    'player_name': player_name,
                    'team': team,
                    'sportsbook': sportsbook,
                    'line_score': float(line_score),
                    'game_start': game_start,
                    'time_scraped': time_scraped,
                    'opponent_team': opponent_team
                }
                
                parsed_props.append(parsed_prop)
        
        except Exception as e:
            print(f"Error processing player {player_id}: {e}", file=sys.stderr)
            continue
    
    return parsed_props

def get_draftedge_df():
    """
    Fetch DraftEdge data and return as DataFrame with standardized schema.
    
    Returns:
        pd.DataFrame: DataFrame with columns ['player_name', 'team', 'sportsbook', 
                      'line_score', 'game_start', 'time_scraped', 'opponent_team']
    """
    try:
        data = fetch_draftedge_data()
        
        if not data:
            print("Warning: No data retrieved from DraftEdge")
            return pd.DataFrame(columns=['player_name', 'team', 'sportsbook', 
                                         'line_score', 'game_start', 'time_scraped', 'opponent_team'])
        
        parsed_props = parse_draftedge_props(data)
        
        if not parsed_props:
            print("Warning: No props found after filtering")
            return pd.DataFrame(columns=['player_name', 'team', 'sportsbook', 
                                         'line_score', 'game_start', 'time_scraped', 'opponent_team'])
        
        # Convert to DataFrame with explicit column order
        df = pd.DataFrame(parsed_props)
        df = df[['player_name', 'team', 'sportsbook', 'line_score', 'game_start', 'time_scraped', 'opponent_team']]
        
        print(f"DraftEdge: Successfully retrieved {len(df)} records from {df['sportsbook'].nunique()} sportsbooks")
        return df
    
    except Exception as e:
        print(f"Error fetching DraftEdge data: {e}", file=sys.stderr)
        return pd.DataFrame(columns=['player_name', 'team', 'sportsbook', 
                                     'line_score', 'game_start', 'time_scraped', 'opponent_team'])

def main():
    """
    Main execution function
    """
    print("Fetching NBA player props from DraftEdge...")
    
    # Fetch and parse data
    df = get_draftedge_df()
    
    print(df.head(20))
    
    if df.empty:
        print("No props found in response")
        sys.exit(1)
    

if __name__ == "__main__":
    main()