import os
import io
import time
import random
import requests
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import cloudinary.uploader
import cloudinary

# === CONFIGURATION ===
CSV_PATH = "mergedf.CSV"
CREDENTIALS_FILE = "google_drive_credentials.json"
IMAGES_FOLDER_ID = "1nVL4PnNrVCeW-qKZ770DsvlJKwdLqF46"
ACCESS_TOKEN = "EAAS1RlssQeUBOZBLFZC7ZBNfHVweaznweuFFrHyeBnLZCZAAUNeQtg0ottd6KVJhPK31CW2pczpYIMHrG84BHZAcccPLe5k1tWsh8sR8wFo05FqIDlsQKv5DnFfWzcZBaYOGvCbGAzs8kMaZAugwuxQ6zqDqkMsLacchgsRkkpzVqAByT6IEjlOu"
IG_USER_ID = "17841467036592820"

# === CLOUDINARY SETUP ===
cloudinary.config(
    cloud_name="dskl1auty",
    api_key="287383892435529",
    api_secret="7ykMGLhlMzxEXVjk-pbP1ZuOGD8"
)

# === Google Drive Setup ===
def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

drive_service = get_drive_service()

# === Download from Drive and Upload to Cloudinary ===
def upload_image_to_cloudinary_from_drive(post_id):
    for ext in ['.jpg', '.jpeg', '.png', '.webp']:
        filename = f"{post_id}{ext}"
        query = f"'{IMAGES_FOLDER_ID}' in parents and name = '{filename}'"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])
        if not files:
            continue

        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)

        # Upload to Cloudinary
        result = cloudinary.uploader.upload(fh, folder="instagram_posts", public_id=post_id, overwrite=True)
        return result['secure_url']
    return None

# === Instagram API functions ===
def upload_image(ig_user_id, access_token, image_url, caption):
    url = f'https://graph.facebook.com/v18.0/{ig_user_id}/media'
    params = {
        'image_url': image_url,
        'caption': caption,
        'access_token': access_token
    }
    response = requests.post(url, data=params)
    result = response.json()
    print(result)
    if 'id' in result:
        return result['id']
    else:
        raise ValueError(f"Upload error: {result.get('error', {}).get('message', 'Unknown')}")

def publish_image(ig_user_id, access_token, media_id):
    url = f'https://graph.facebook.com/v18.0/{ig_user_id}/media_publish'
    params = {
        'creation_id': media_id,
        'access_token': access_token
    }
    response = requests.post(url, params=params)
    return response.json()

def upload_and_publish_image(ig_user_id, access_token, image_url, caption):
    try:
        media_id = upload_image(ig_user_id, access_token, image_url, caption)
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

    for _ in range(10):
        result = publish_image(ig_user_id, access_token, media_id)
        if 'id' in result:
            print("✅ Image published!")
            return True
        else:
            print("⏳ Not ready yet. Retrying...")
            time.sleep(15)

    print("❌ Timed out publishing.")
    return False

# === Main Logic ===
def post_random_image():
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()

    if 'Posted' not in df.columns:
        df['Posted'] = ''

    # Filter only unposted image posts
        # Only pick rows where it's an image (Image URL is non-empty, Video URL is empty)
    unposted = df[
        (df['Posted'] != 'YES') &
        df['Image URL'].notna() & (df['Image URL'].str.strip() != '') &
        (df['Video URL'].isna() | (df['Video URL'].str.strip() == ''))
    ]


    if unposted.empty:
        print("✅ All images posted.")
        return

    row = unposted.sample(1).iloc[0]
    idx = row.name
    post_id = str(row['Post ID'])

    image_url = upload_image_to_cloudinary_from_drive(post_id)
    if not image_url:
        print(f"❌ Image not found for Post ID: {post_id}")
        df.at[idx, 'Posted'] = 'SKIPPED'
        df.to_csv(CSV_PATH, index=False)
        return

    # Build caption
    caption = str(row.get('Caption', '')).strip()
    tagged = str(row.get('Tagged Users', '')).strip()
    account = str(row.get('Account Username', '')).strip()
    credits = " ".join(filter(None, [tagged, f"@{account}" if account else ""]))

    full_caption = f"""{caption}
.
.
.
Credits: {credits}
.
.
.
#food #foodie"""

    print(f"📷 Uploading image for post ID {post_id}")
    success = upload_and_publish_image(IG_USER_ID, ACCESS_TOKEN, image_url, full_caption)

    if success:
        df.at[idx, 'Posted'] = 'YES'
        df.to_csv(CSV_PATH, index=False)
        print("✅ Image posted and CSV updated.")
    else:
        print("❌ Post failed.")

# === Run It ===
post_random_image()
