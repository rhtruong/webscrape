from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import pickle

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file']

CREDENTIALS_PATH = 'credentials/google_drive_api_creds.json'
TOKEN_PATH = 'credentials/token.pickle'

def authenticate_google_drive():
    """
    Authenticate with Google Drive API
    Returns: Google Drive service object
    """
    creds = None
    
    # Check if token already exists
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, let user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing expired credentials...")
            creds.refresh(Request())
        else:
            logger.info("Starting OAuth flow for first-time authentication...")
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
        logger.info("Credentials saved successfully")
    
    # Build the service
    service = build('drive', 'v3', credentials=creds)
    logger.info("Google Drive authentication successful")
    return service

def create_folder_if_not_exists(service, folder_name='NBA_Props'):
    """
    Create a folder in Google Drive if it doesn't exist
    Args:
        service: Google Drive service object
        folder_name (str): Name of folder to create
    Returns: str: Folder ID
    """
    # Search for existing folder
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, fields='files(id, name)').execute()
    folders = results.get('files', [])
    
    if folders:
        folder_id = folders[0]['id']
        logger.info(f"Found existing folder '{folder_name}' with ID: {folder_id}")
        return folder_id
    
    # Create new folder if not found
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = service.files().create(body=file_metadata, fields='id').execute()
    folder_id = folder.get('id')
    logger.info(f"Created new folder '{folder_name}' with ID: {folder_id}")
    return folder_id

def upload_file_to_drive(service, filepath, folder_id):
    """
    Upload a file to Google Drive
    Args:
        service: Google Drive service object
        filepath (str): Path to file to upload
        folder_id (str): ID of folder to upload to
    Returns: dict: File metadata or None if failed
    """
    try:
        filename = os.path.basename(filepath)
        
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(filepath, mimetype='text/csv', resumable=True)
        
        logger.info(f"Uploading {filename} to Google Drive...")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()
        
        logger.info(f"Upload successful! File ID: {file.get('id')}")
        logger.info(f"View at: {file.get('webViewLink')}")
        return file
    
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return None

def upload_csv_to_drive(filepath):
    """
    Main function to upload CSV to Google Drive
    Args: filepath (str): Path to CSV file
    Returns: bool: True if successful, False otherwise
    """
    try:
        # Authenticate
        service = authenticate_google_drive()
        
        # Create/find folder
        folder_id = create_folder_if_not_exists(service)
        
        # Upload file
        result = upload_file_to_drive(service, filepath, folder_id)
        
        return result is not None
    
    except Exception as e:
        logger.error(f"Error in upload process: {e}")
        return False

def main():
    """Test upload function"""
    # Example: upload a test file
    test_file = "data/nba_bettingpros_2025_10_30.csv"
    
    if os.path.exists(test_file):
        success = upload_csv_to_drive(test_file)
        if success:
            logger.info("Test upload completed successfully")
        else:
            logger.error("Test upload failed")
    else:
        logger.warning(f"Test file {test_file} not found")

    return filepath

if __name__ == "__main__":
    main()