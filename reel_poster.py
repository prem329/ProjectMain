import time
import requests
import pandas as pd
import random
from google.oauth2 import service_account
from googleapiclient.discovery import build

# === CONFIGURATION ===
CSV_PATH = "mergedf.CSV"
CREDENTIALS_FILE = "google_drive_credentials.json"
ACCESS_TOKEN = "EAAS1RlssQeUBOZBLFZC7ZBNfHVweaznweuFFrHyeBnLZCZAAUNeQtg0ottd6KVJhPK31CW2pczpYIMHrG84BHZAcccPLe5k1tWsh8sR8wFo05FqIDlsQKv5DnFfWzcZBaYOGvCbGAzs8kMaZAugwuxQ6zqDqkMsLacchgsRkkpzVqAByT6IEjlOu"
IG_USER_ID = "17841467036592820"
VIDEOS_FOLDER_ID = "16wvWPSRsCRSBhoB9R12T9igTEz9Flu84"  # ID of the 'videos' subfolder


# === Google Drive Setup ===
def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

drive_service = get_drive_service()

def get_public_drive_video_link(post_id):
    filename = f"{post_id}.mp4"
    query = f"'{VIDEOS_FOLDER_ID}' in parents and name = '{filename}'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        print(f"❌ Video not found in Drive for Post ID: {post_id}")
        return None

    file_id = files[0]['id']

    # Make the file public
    drive_service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        fields="id"
    ).execute()

    # Generate public downloadable link
    return f"https://drive.google.com/uc?id={file_id}&export=download"

# === Instagram Posting ===
def upload_video(ig_user_id, access_token, video_url, caption):
    url = f'https://graph-video.facebook.com/v18.0/{ig_user_id}/media'
    params = {
        'access_token': access_token,
        'video_url': video_url,
        'caption': caption,
        'media_type': 'REELS'
    }
    response = requests.post(url, params=params)
    result = response.json()
    print(result)
    if 'id' in result:
        return result['id']
    else:
        raise ValueError(f"Upload error: {result.get('error', {}).get('message', 'Unknown')}")

def publish_video(ig_user_id, access_token, media_id):
    url = f'https://graph.facebook.com/v18.0/{ig_user_id}/media_publish'
    params = {
        'access_token': access_token,
        'creation_id': media_id
    }
    response = requests.post(url, params=params)
    return response.json()

def upload_and_publish_video(ig_user_id, access_token, video_url, caption):
    try:
        media_id = upload_video(ig_user_id, access_token, video_url, caption)
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

    for _ in range(10):
        result = publish_video(ig_user_id, access_token, media_id)
        if 'id' in result:
            print("✅ Reel published!")
            return True
        else:
            print("⏳ Not ready yet. Retrying...")
            time.sleep(15)

    print("❌ Timed out publishing.")
    return False

# === Main Logic ===
def post_random_reel():
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()

    if 'Posted' not in df.columns:
        df['Posted'] = ''

    # Filter: Unposted + has Video URL
    unposted = df[(df['Posted'] != 'YES') & df['Video URL'].notna() & (df['Video URL'].str.strip() != '')]
    if unposted.empty:
        print("✅ All reels posted.")
        return

    row = unposted.sample(1).iloc[0]
    idx = row.name
    post_id = str(row['Post ID'])

    video_url = get_public_drive_video_link(post_id)
    if not video_url:
        df.at[idx, 'Posted'] = 'SKIPPED'
        df.to_csv(CSV_PATH, index=False)
        return

    # Build caption
    caption = str(row.get('Caption', '')).strip()
    tagged_users = str(row.get('Tagged Users', '')).strip()
    account_username = str(row.get('Account Username', '')).strip()
    credit_line = " ".join(filter(None, [tagged_users, f"@{account_username}" if account_username else ""]))

    full_caption = f"""{caption}
.
.
.
Credits: {credit_line}
.
.
.
#food #foodie"""

    print(f"📤 Uploading reel for post ID {post_id}")
    success = upload_and_publish_video(IG_USER_ID, ACCESS_TOKEN, video_url, full_caption)

    if success:
        df.at[idx, 'Posted'] = 'YES'
        df.to_csv(CSV_PATH, index=False)
        print("✅ CSV updated!")
    else:
        print("❌ Posting failed.")

# === Run it ===
post_random_reel()
