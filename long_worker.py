"""
GitHub Cloud Long Anime Worker (All-in-One: 4 Parallel Episodes + Subtitle Engine)
----------------------------------------------------------------------------------
Runs inside GitHub Actions (2 vCPU, 7 GB RAM, 14 GB SSD, 1Gbps Network).
- Downloads anime episodes via aria2c (4 parallel workers)
- Automatic fallback from Main magnet to multiple Backup magnets
- Uploads video to RPMShare Server 2 via TUS
- Subtitle Pipeline:
  * Extracts embedded subtitles from local MKV via ffmpeg (fallback to RPM API)
  * Cleans English dialogs and uploads English.srt to GitHub Releases (DDL)
  * Translates to Sinhala with Spoken Sinhala dictionary & 5 translation threads
  * Uploads Sinhala.srt to GitHub Releases (DDL)
  * Attaches Sinhala sub to RPMShare video via API
  * Marks episode document in Firestore directly as 'uploaded'
  * Clears missing sub alerts in RTDB
- Immediate disk space cleanup after each episode
"""

import os
import sys
import json
import time
import uuid
import re
import shutil
import subprocess
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pysubs2
from dotenv import load_dotenv
from torrentool.api import Torrent
import firebase_admin
from firebase_admin import credentials, firestore, db as rtdb

# Ensure sub_engine can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

try:
    from sub_engine import (
        process_sinhala_sub,
        process_english_sub,
        upload_to_github_release,
        upload_sub_to_rpm,
        delete_existing_sinhala_subs,
        clear_missing_sub_alert,
        push_missing_sub_alert,
        detect_encoding,
        is_valid_sub_file,
        MIN_SUB_LINE_THRESHOLD
    )
except ImportError:
    try:
        from long_manager.sub_engine import (
            process_sinhala_sub,
            process_english_sub,
            upload_to_github_release,
            upload_sub_to_rpm,
            delete_existing_sinhala_subs,
            clear_missing_sub_alert,
            push_missing_sub_alert,
            detect_encoding,
            is_valid_sub_file,
            MIN_SUB_LINE_THRESHOLD
        )
    except ImportError:
        MIN_SUB_LINE_THRESHOLD = 125

load_dotenv()
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

WORKER_ID = f"CloudLong-{uuid.uuid4().hex[:6]}"
API_TOKEN_2 = os.getenv("RPMSHARE_API_TOKEN_2", "")
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL", "https://anishift-5d14b-default-rtdb.firebaseio.com/")
RTDB_LONG_NODE = "long_batch_jobs"
BASE_DOWNLOAD_DIR = "downloads_long"
MAX_CONCURRENT_EPISODES = int(os.getenv("LONG_CONCURRENT_EPISODES", 4))

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [{WORKER_ID}] {msg}", flush=True)

def init_firebase():
    key_candidates = [
        os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
        "serviceAccountKey.json",
        os.path.join(os.path.dirname(__file__), "..", "serviceAccountKey.json"),
    ]
    key_path = next((p for p in key_candidates if os.path.exists(p)), None)

    if not key_path:
        firebase_json = os.getenv("FIREBASE_JSON")
        if firebase_json:
            key_path = "serviceAccountKey.json"
            with open(key_path, "w", encoding="utf-8") as f:
                f.write(firebase_json)

    if not key_path or not os.path.exists(key_path):
        raise RuntimeError(f"[{WORKER_ID}] ❌ serviceAccountKey.json not found!")

    if not firebase_admin._apps:
        cred = credentials.Certificate(key_path)
        firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_DB_URL})

    return firestore.client()

def get_rtdb_ref(path=""):
    try:
        return rtdb.reference(path)
    except Exception:
        return rtdb.reference(path, url=FIREBASE_DB_URL)

# ==========================================
# 🧠 1. FILTERS & EXTRACTION
# ==========================================
def is_junk_file(filename):
    f_lower = filename.lower()
    junk_pattern = r'\b(movie|special|ova|ncop|nced|opening|ending|recap|preview|batch|log|digest|picture drama|sp\d*)\b'
    if re.search(junk_pattern, f_lower): return True
    if "episode of" in f_lower: return True
    return False

def clean_filename(filename):
    clean_name = filename.lower()
    clean_name = re.sub(r'\[.*?\]', ' ', clean_name)
    clean_name = re.sub(r'\(.*?\)', ' ', clean_name)
    clean_name = re.sub(r'\b(1080p|720p|480p|x264|x265|h264|hevc|10bit|8bit)\b', ' ', clean_name)
    return clean_name

def extract_episode_number(filename):
    clean_name = clean_filename(filename)
    match = re.search(r'[sS]\d+[eE]0*(\d+)', clean_name)
    if match: return int(match.group(1))
    matches = re.findall(r'\s-\s0*(\d{1,4})(?:v\d)?\b', clean_name)
    if matches: return int(matches[0])
    match = re.search(r'\b(?:ep|episode)\.?\s?0*(\d+)\b', clean_name)
    if match: return int(match.group(1))
    return None

# ==========================================
# ⚙️ 2. TORRENT METADATA RESOLVER
# ==========================================
def download_torrent_metadata(magnet_link, work_dir="."):
    if not magnet_link:
        return None
    match = re.search(r'xt=urn:btih:([a-zA-Z0-9]+)', magnet_link)
    if not match:
        log(f"❌ Invalid magnet link format: {magnet_link}")
        return None
    magnet_hash = match.group(1).lower()
    torrent_file = os.path.join(work_dir, f"{magnet_hash}.torrent")

    if os.path.exists(torrent_file) and os.path.getsize(torrent_file) > 1000:
        with open(torrent_file, 'r', errors='ignore') as f:
            if "<" not in f.read(10):
                log(f"✅ Metadata already exists for {magnet_hash}")
                return torrent_file
            else:
                os.remove(torrent_file)

    log(f"📥 Resolving Torrent Metadata ({magnet_hash})...")
    cache_urls = [
        f"https://itorrents.org/torrent/{magnet_hash}.torrent",
        f"https://btcache.me/torrent/{magnet_hash}",
        f"https://torrage.info/torrent.php?h={magnet_hash}"
    ]

    downloaded = False
    for url in cache_urls:
        try:
            r = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200 and len(r.content) > 1000 and not r.content.startswith(b'<!DOCTYPE') and not r.content.startswith(b'<html'):
                with open(torrent_file, 'wb') as f:
                    f.write(r.content)
                downloaded = True
                log("   ✅ Cache metadata resolved!")
                break
        except Exception:
            pass

    if not downloaded:
        log("   🌐 Fetching metadata via aria2c DHT...")
        subprocess.run([
            'aria2c',
            '--bt-metadata-only=true',
            '--bt-save-metadata=true',
            '--allow-overwrite=true',
            '--seed-time=0',
            '--bt-stop-timeout=90',
            f'--dir={work_dir}',
            magnet_link
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if os.path.exists(torrent_file) and os.path.getsize(torrent_file) > 1000:
            downloaded = True
            log("   ✅ Magnet metadata resolved via DHT!")

    return torrent_file if downloaded else None

def scan_torrent_files(torrent_file):
    try:
        my_torrent = Torrent.from_file(torrent_file)
    except Exception as e:
        log(f"❌ Failed to parse torrent file {torrent_file}: {e}")
        return None

    file_map = {}
    for idx, f in enumerate(my_torrent.files, start=1):
        if any(f.name.lower().endswith(ext) for ext in ['.mkv', '.mp4']):
            if not is_junk_file(f.name):
                ep_num = extract_episode_number(os.path.basename(f.name))
                if ep_num:
                    file_map[ep_num] = {'index': idx, 'name': os.path.basename(f.name)}
    return file_map

# ==========================================
# 🚀 3. TUS UPLOADER (RPMShare Server 2)
# ==========================================
def upload_via_tus(file_path, folder_id):
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            file_size = os.path.getsize(file_path)
            headers = {"api-token": API_TOKEN_2}
            resp = requests.get("https://rpmshare.com/api/v1/video/upload", headers=headers, timeout=25).json()
            tus_url, access_token = resp['tusUrl'], resp['accessToken']
            file_name = os.path.basename(file_path)

            from tusclient import client
            my_client = client.TusClient(tus_url)
            metadata = {
                'accessToken': access_token,
                'filename': file_name,
                'filetype': 'video/x-matroska',
                'folderId': str(folder_id)
            }
            uploader = my_client.uploader(file_path=file_path, metadata=metadata, chunk_size=52428800)

            while uploader.offset < file_size:
                uploader.upload_chunk()

            time.sleep(6)
            search_resp = requests.get(
                f"https://rpmshare.com/api/v1/video/manage?search={urllib.parse.quote(file_name)}",
                headers=headers, timeout=25
            ).json()
            video_id = search_resp['data'][0]['id'] if 'data' in search_resp and search_resp['data'] else None
            if video_id:
                return video_id

            log(f"⚠️ Video ID not immediately indexed (attempt {attempt}/{max_retries}). Retrying in 5s...")
            time.sleep(5)
            search_resp2 = requests.get(
                f"https://rpmshare.com/api/v1/video/manage?search={urllib.parse.quote(file_name)}",
                headers=headers, timeout=25
            ).json()
            video_id2 = search_resp2['data'][0]['id'] if 'data' in search_resp2 and search_resp2['data'] else None
            if video_id2:
                return video_id2

        except Exception as e:
            log(f"❌ Upload Attempt {attempt}/{max_retries} Error: {e}")
            if attempt < max_retries:
                time.sleep(5 * attempt)
    return None

# ==========================================
# 💬 4. SUBTITLE EXTRACTION & PIPELINE
# ==========================================
# ==========================================
# 💬 4. SUBTITLE EXTRACTION & PIPELINE
# ==========================================
def extract_sub_from_local_video(video_path, work_dir):
    """Extracts all embedded subtitle tracks from local MKV/MP4 using ffprobe & ffmpeg,
    scores each track using the 125-line threshold & language weighting system,
    and returns (winner_si_path, winner_other_path)."""
    try:
        streams = []
        try:
            probe_cmd = [
                'ffprobe', '-v', 'error',
                '-select_streams', 's',
                '-show_entries', 'stream=index:stream_tags=language,title',
                '-of', 'json', video_path
            ]
            res = subprocess.run(probe_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=25)
            if res.returncode == 0:
                p_data = json.loads(res.stdout)
                streams = p_data.get('streams', [])
        except Exception:
            pass

        if not streams:
            streams = [{'index': f"s:{i}", 'tags': {'title': f"Track {i}"}} for i in range(6)]

        si_candidates = []
        other_candidates = []

        for s in streams:
            s_idx = s.get('index')
            tags = s.get('tags', {}) or {}
            lang = tags.get('language', '').lower()
            name = tags.get('title', f"Track {s_idx}")
            name_lower = name.lower()

            out_srt = os.path.join(work_dir, f"embedded_sub_{s_idx}_{uuid.uuid4().hex[:4]}.srt")
            map_arg = f"0:{s_idx}" if str(s_idx).startswith('s:') or ':' in str(s_idx) else f"0:{s_idx}"

            cmd = [
                'ffmpeg', '-y', '-i', video_path,
                '-map', map_arg,
                '-c:s', 'srt',
                out_srt
            ]
            try:
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=45)
            except Exception:
                continue

            if not os.path.exists(out_srt) or os.path.getsize(out_srt) < 300:
                if os.path.exists(out_srt): os.remove(out_srt)
                continue

            if not is_valid_sub_file(out_srt):
                if os.path.exists(out_srt): os.remove(out_srt)
                continue

            try:
                enc = detect_encoding(out_srt)
                try: subs = pysubs2.load(out_srt, encoding=enc)
                except Exception: subs = pysubs2.load(out_srt, encoding='latin-1')

                lines = len(subs.events)

                # --- 🟢 125-Line Threshold & Subtitle Scoring Logic ---
                score = lines
                if lines >= MIN_SUB_LINE_THRESHOLD:
                    if any(x in name_lower for x in ['si', 'sinhala', 'සිංහල']) or lang == 'si':
                        score += 200000
                    elif any(x in name_lower for x in ['en', 'eng', 'english']) or lang == 'en':
                        score += 100000
                    elif any(x in name_lower for x in ['ja', 'jap', 'romaji']) or lang == 'ja':
                        score -= 100000
                    elif any(x in name_lower for x in ['sign', 'song', 'forced']):
                        score -= 100000
                    else:
                        score += 10000  # Valid other language (French, Spanish, etc.)
                else:
                    score -= 50000  # Penalize cracked/incomplete tracks

                log(f"   🎬 Embedded Sub '{name}' | Lang: {lang or 'N/A'} | Lines: {lines} | Score: {score}")

                if score <= 0:
                    if os.path.exists(out_srt): os.remove(out_srt)
                    continue

                track_info = {
                    'path': out_srt,
                    'lines': lines,
                    'score': score,
                    'name': name,
                    'lang': lang
                }

                if any(x in name_lower for x in ['si', 'sinhala', 'සිංහල']) or lang == 'si':
                    si_candidates.append(track_info)
                else:
                    other_candidates.append(track_info)

            except Exception:
                if os.path.exists(out_srt):
                    try: os.remove(out_srt)
                    except Exception: pass

        winner_si_path = None
        if si_candidates:
            si_candidates.sort(key=lambda x: x['score'], reverse=True)
            winner_si = si_candidates[0]
            winner_si_path = winner_si['path']
            log(f"🏆 WINNER EMBEDDED SINHALA: '{winner_si['name']}' ({winner_si['lines']} lines, Score: {winner_si['score']})")
            for c in si_candidates[1:]:
                if os.path.exists(c['path']):
                    try: os.remove(c['path'])
                    except Exception: pass

        winner_cand_path = None
        if other_candidates:
            other_candidates.sort(key=lambda x: x['score'], reverse=True)
            winner_cand = other_candidates[0]
            winner_cand_path = winner_cand['path']
            log(f"🏆 WINNER EMBEDDED TRANSLATION SOURCE: '{winner_cand['name']}' ({winner_cand['lines']} lines, Score: {winner_cand['score']})")
            for c in other_candidates[1:]:
                if os.path.exists(c['path']):
                    try: os.remove(c['path'])
                    except Exception: pass

        return winner_si_path, winner_cand_path
    except Exception as e:
        log(f"⚠️ Embedded sub extraction error: {e}")
        return None, None

def download_rpm_sub_api(video_id, work_dir):
    """Fallback: Downloads and scores subtitle tracks from RPMShare video files API,
    returning (winner_si_path, winner_other_path)."""
    headers = {'api-token': API_TOKEN_2}
    target_host = "https://rpmshare.com"
    try:
        p_resp = requests.get("https://rpmshare.com/api/v1/video/player/default", headers=headers, timeout=10)
        if p_resp.status_code == 200:
            p_dom = p_resp.json().get('domain')
            if p_dom: target_host = f"https://{p_dom}" if not p_dom.startswith("http") else p_dom
    except Exception:
        pass

    for attempt in range(4):
        try:
            time.sleep(4)
            f_resp = requests.get(f"https://rpmshare.com/api/v1/video/manage/{video_id}/files", headers=headers, timeout=20)
            if f_resp.status_code == 200:
                files = f_resp.json()
                candidates = [f for f in files if f.get('type') == 'Subtitle']
                if candidates:
                    log(f"🔍 Analyzing {len(candidates)} tracks from RPM (Server 2)...")
                    si_candidates = []
                    other_candidates = []

                    for c in candidates:
                        try:
                            dl_resp = requests.get(f"{target_host}{c['url']}", timeout=30)
                            if dl_resp.status_code == 200:
                                ext = c.get('extension', 'srt')
                                temp_path = os.path.join(work_dir, f"rpm_sub_{uuid.uuid4().hex[:6]}.{ext}")
                                with open(temp_path, "wb") as f:
                                    f.write(dl_resp.content)

                                if not is_valid_sub_file(temp_path):
                                    if os.path.exists(temp_path): os.remove(temp_path)
                                    continue

                                enc = detect_encoding(temp_path)
                                try: subs = pysubs2.load(temp_path, encoding=enc)
                                except Exception: subs = pysubs2.load(temp_path, encoding='latin-1')

                                lines = len(subs.events)
                                name = c.get('name', 'Unnamed')
                                name_lower = name.lower()
                                lang = c.get('language', '').lower()

                                # --- 🟢 125-Line Threshold & Subtitle Scoring Logic ---
                                score = lines
                                if lines >= MIN_SUB_LINE_THRESHOLD:
                                    if any(x in name_lower for x in ['si', 'sinhala', 'සිංහල']) or lang == 'si':
                                        score += 200000
                                    elif any(x in name_lower for x in ['en', 'eng', 'english']) or lang == 'en':
                                        score += 100000
                                    elif any(x in name_lower for x in ['ja', 'jap', 'romaji']) or lang == 'ja':
                                        score -= 100000
                                    elif any(x in name_lower for x in ['sign', 'song', 'forced']):
                                        score -= 100000
                                    else:
                                        score += 10000
                                else:
                                    score -= 50000

                                log(f"   📄 RPM Track '{name}' | Lang: {lang or 'N/A'} | Lines: {lines} | Score: {score}")

                                if score <= 0:
                                    if os.path.exists(temp_path): os.remove(temp_path)
                                    continue

                                track_info = {
                                    'path': temp_path,
                                    'lines': lines,
                                    'score': score,
                                    'name': name,
                                    'lang': lang
                                }

                                if any(x in name_lower for x in ['si', 'sinhala', 'සිංහල']) or lang == 'si':
                                    si_candidates.append(track_info)
                                else:
                                    other_candidates.append(track_info)
                        except Exception:
                            pass

                    winner_si_path = None
                    if si_candidates:
                        si_candidates.sort(key=lambda x: x['score'], reverse=True)
                        winner_si_path = si_candidates[0]['path']
                        log(f"🏆 WINNER RPM SINHALA: '{si_candidates[0]['name']}' ({si_candidates[0]['lines']} lines, Score: {si_candidates[0]['score']})")
                        for loser in si_candidates[1:]:
                            if os.path.exists(loser['path']):
                                try: os.remove(loser['path'])
                                except Exception: pass

                    winner_other_path = None
                    if other_candidates:
                        other_candidates.sort(key=lambda x: x['score'], reverse=True)
                        winner_other_path = other_candidates[0]['path']
                        log(f"🏆 WINNER RPM TRANSLATION SOURCE: '{other_candidates[0]['name']}' ({other_candidates[0]['lines']} lines, Score: {other_candidates[0]['score']})")
                        for loser in other_candidates[1:]:
                            if os.path.exists(loser['path']):
                                try: os.remove(loser['path'])
                                except Exception: pass

                    return winner_si_path, winner_other_path
        except Exception:
            pass
    return None, None

def process_episode_subtitles(ep_num, video_path, video_id, anime_id, title, work_dir):
    """Extracts, cleans, translates to Sinhala, and uploads both subs to GitHub Releases & RPM"""
    log(f"💬 Ep {ep_num} Extracting Subtitles with 125-Line Scoring System...")
    si_src = None
    other_src = None

    if video_path and os.path.exists(video_path):
        si_src, other_src = extract_sub_from_local_video(video_path, work_dir)

    if not si_src and not other_src and video_id:
        si_src, other_src = download_rpm_sub_api(video_id, work_dir)

    si_url = None
    en_url = None
    rel_ctx = {}

    # If an existing valid Sinhala sub was discovered
    if si_src:
        log(f"🎉 Ep {ep_num} Existing Sinhala Sub Found ({si_src})! Uploading directly...")
        si_url = upload_to_github_release(si_src, asset_name="Sinhala.srt", release_context=rel_ctx)
        delete_existing_sinhala_subs(video_id, api_token=API_TOKEN_2)
        upload_sub_to_rpm(video_id, si_src, api_token=API_TOKEN_2, remote_url=si_url)
        clear_missing_sub_alert(rtdb, anime_id, ep_num)
        log(f"🎉 Ep {ep_num} Sinhala Sub Attached to RPM & GitHub: {si_url}")

    # Process English or other source track for Sinhala translation
    if other_src:
        try:
            en_processed = process_english_sub(other_src, log_prefix=f"[{WORKER_ID} Ep {ep_num}]")
            target_en = en_processed if en_processed else other_src
            en_url = upload_to_github_release(target_en, asset_name="English.srt", release_context=rel_ctx)
            if en_url:
                log(f"✅ Ep {ep_num} English Sub Online: {en_url}")

            if not si_url:
                log(f"🔄 Ep {ep_num} Translating Subtitle to Sinhala (Auto Source Detection)...")
                out_si_name = os.path.join(work_dir, f"sinhala_{uuid.uuid4().hex[:4]}.srt")
                si_processed = process_sinhala_sub(
                    target_en,
                    out_name=out_si_name,
                    max_workers=5,
                    log_prefix=f"[{WORKER_ID} Ep {ep_num}]"
                )
                if si_processed:
                    si_url = upload_to_github_release(si_processed, asset_name="Sinhala.srt", release_context=rel_ctx)
                    delete_existing_sinhala_subs(video_id, api_token=API_TOKEN_2)
                    upload_sub_to_rpm(video_id, si_processed, api_token=API_TOKEN_2, remote_url=si_url)
                    clear_missing_sub_alert(rtdb, anime_id, ep_num)
                    log(f"🎉 Ep {ep_num} Sinhala Sub Attached to RPM & GitHub: {si_url}")
        except Exception as e:
            log(f"⚠️ Subtitle translation/upload error on Ep {ep_num}: {e}")

    if not si_url:
        log(f"⚠️ Ep {ep_num} No Sinhala Sub generated. Alerting Admin Panel.")
        push_missing_sub_alert(rtdb, anime_id, title, ep_num, video_id, 2)

    return si_url, en_url

# ==========================================
# ⚡ 5. PARALLEL SINGLE-EPISODE WORKER
# ==========================================
def process_single_episode(ep_num, main_torrent_file, main_file_map, backup_maps, folder_id, anime_id, title, collection_name, db):
    """Downloads, uploads to RPM, extracts/translates subs, and marks uploaded"""
    ep_dir = os.path.join(BASE_DOWNLOAD_DIR, f"ep_{ep_num:04d}")
    os.makedirs(ep_dir, exist_ok=True)
    uploaded_successfully = False
    video_id = None
    source_used = None
    found_video_path = None

    try:
        # A. Try Main Magnet
        if ep_num in main_file_map:
            target = main_file_map[ep_num]
            log(f"🎬 Ep {ep_num} Downloading from Main Magnet: {target['name']}")
            subprocess.run([
                'aria2c', '--seed-time=0', '--bt-stop-timeout=180',
                '--file-allocation=none', '--summary-interval=0',
                f'--dir={ep_dir}', f'--select-file={target["index"]}', main_torrent_file
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            for root, dirs, files in os.walk(ep_dir):
                if target['name'] in files:
                    found_video_path = os.path.join(root, target['name'])
                    break

            if found_video_path and os.path.exists(found_video_path) and os.path.getsize(found_video_path) > 1024 * 1024:
                log(f"🚀 Ep {ep_num} Uploading video to RPMShare (Server 2)...")
                video_id = upload_via_tus(found_video_path, folder_id)
                if video_id:
                    uploaded_successfully = True
                    source_used = "main"

        # B. Fallback to Backup Magnets if Main failed
        if not uploaded_successfully:
            for b_idx, b_info in enumerate(backup_maps):
                if b_info and ep_num in b_info['file_map']:
                    target = b_info['file_map'][ep_num]
                    b_torrent = b_info['torrent_file']
                    log(f"🔄 Ep {ep_num} Fallback to Backup Magnet #{b_idx+1}: {target['name']}")

                    shutil.rmtree(ep_dir, ignore_errors=True)
                    os.makedirs(ep_dir, exist_ok=True)

                    subprocess.run([
                        'aria2c', '--seed-time=0', '--bt-stop-timeout=180',
                        '--file-allocation=none', '--summary-interval=0',
                        f'--dir={ep_dir}', f'--select-file={target["index"]}', b_torrent
                    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                    for root, dirs, files in os.walk(ep_dir):
                        if target['name'] in files:
                            found_video_path = os.path.join(root, target['name'])
                            break

                    if found_video_path and os.path.exists(found_video_path) and os.path.getsize(found_video_path) > 1024 * 1024:
                        log(f"🚀 Ep {ep_num} (Backup #{b_idx+1}) Uploading video to RPMShare...")
                        video_id = upload_via_tus(found_video_path, folder_id)
                        if video_id:
                            uploaded_successfully = True
                            source_used = f"backup_{b_idx+1}"
                            break

        if uploaded_successfully and video_id:
            # 💬 Execute Subtitle Pipeline directly on this episode!
            si_url, en_url = process_episode_subtitles(ep_num, found_video_path, video_id, anime_id, title, ep_dir)

            ep_doc_id = f"episode_{ep_num:04d}"
            db.collection(collection_name).document(str(anime_id)).collection('episodes').document(ep_doc_id).update({
                'status': 'uploaded',  # Directly marked uploaded with subtitles!
                'links.rpm_video_id': video_id,
                'links.rpm_stream': f"https://rpmshare.com/v/{video_id}",
                'server': 2,
                'subtitles.sinhala': si_url if si_url else 'not_found',
                'subtitles.english': en_url if en_url else 'not_found',
                'last_updated': firestore.SERVER_TIMESTAMP
            })
            log(f"✨ Ep {ep_num} 100% COMPLETE! Video ID: {video_id} | Sinhala Sub: {si_url or 'None'}")
            return {'ep_num': ep_num, 'status': 'success', 'video_id': video_id, 'source': source_used}
        else:
            log(f"❌ Ep {ep_num} FAILED across all available magnets.")
            return {'ep_num': ep_num, 'status': 'failed'}

    finally:
        # Guarantee immediate disk space cleanup after each episode completes
        shutil.rmtree(ep_dir, ignore_errors=True)

# ==========================================
# 🎯 6. MEGA BATCH ORCHESTRATOR
# ==========================================
def execute_cloud_mega_batch(db, anime_id, collection_name="anime_series"):
    series_ref = db.collection(collection_name).document(str(anime_id))
    snap = series_ref.get()
    if not snap.exists:
        log(f"❌ Series document {anime_id} not found!")
        return False

    series_data = snap.to_dict() or {}
    title = series_data.get('title', {}).get('english') or series_data.get('title', {}).get('romaji') or f"Anime {anime_id}"
    folder_id = series_data.get('rpm_folder_id')
    magnet_link = series_data.get('custom_batch_url')
    backup_magnets = [m for m in series_data.get('backup_magnets', []) if m]

    total_eps = int(series_data.get('episodes_total', 0))
    last_uploaded_ep = int(series_data.get('last_uploaded_ep', 0))
    ram_tracker = last_uploaded_ep

    log("==================================================")
    log(f"⚡ Cloud Mega Batch Starting: {title} (ID: {anime_id})")
    log(f"📊 Starting Ep: {last_uploaded_ep + 1} | Total Eps: {total_eps}")
    log(f"🔥 Parallel Concurrency: {MAX_CONCURRENT_EPISODES} episodes simultaneously")
    log("💬 Integrated Subtitle Pipeline: ACTIVE (Direct 'uploaded' status)")
    log("==================================================")

    # Initial RTDB status
    try:
        get_rtdb_ref(f"{RTDB_LONG_NODE}/{anime_id}").update({
            "status": "processing",
            "anime_id": int(anime_id) if str(anime_id).isdigit() else anime_id,
            "title": title,
            "current_ep": ram_tracker + 1,
            "total_uploaded": ram_tracker,
            "total_episodes": total_eps,
            "message": f"🚀 Cloud Worker processing 4 parallel episodes & subtitles for {title}...",
            "updated_at": int(time.time() * 1000)
        })
    except Exception as e:
        log(f"⚠️ RTDB error: {e}")

    shutil.rmtree(BASE_DOWNLOAD_DIR, ignore_errors=True)
    os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)

    # 1. Download Main Torrent Metadata
    main_torrent_file = download_torrent_metadata(magnet_link, work_dir=".")
    if not main_torrent_file:
        log("❌ Failed to resolve main torrent metadata.")
        next_needed = ram_tracker + 1
        series_ref.update({'status': 'awaiting_admin_batch', 'next_episode': next_needed})
        return False

    main_file_map = scan_torrent_files(main_torrent_file) or {}

    # 2. Download Backup Torrents Metadata
    backup_maps = []
    for b_idx, b_mag in enumerate(backup_magnets):
        b_file = download_torrent_metadata(b_mag, work_dir=".")
        if b_file:
            b_map = scan_torrent_files(b_file)
            if b_map:
                backup_maps.append({'torrent_file': b_file, 'file_map': b_map})

    available_eps = set(main_file_map.keys())
    for b in backup_maps:
        available_eps.update(b['file_map'].keys())

    batch_max_ep = max(available_eps) if available_eps else ram_tracker
    if not total_eps or total_eps < batch_max_ep:
        total_eps = batch_max_ep

    start_ep = ram_tracker + 1
    end_ep = min(batch_max_ep, total_eps)

    if start_ep > end_ep:
        log(f"ℹ️ All episodes up to {batch_max_ep} are already uploaded.")
        series_ref.update({'status': 'completed' if ram_tracker >= total_eps else 'awaiting_admin_batch'})
        return True

    batch_uploaded_count = 0
    batch_failed_eps = []
    all_episodes_to_process = list(range(start_ep, end_ep + 1))

    # Process in chunks of 4 concurrent episodes
    chunk_size = MAX_CONCURRENT_EPISODES
    for i in range(0, len(all_episodes_to_process), chunk_size):
        chunk = all_episodes_to_process[i:i + chunk_size]
        log(f"\n⚡ Launching Parallel Batch: Episodes {chunk}...")

        futures = {}
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_EPISODES) as executor:
            for ep_num in chunk:
                f = executor.submit(
                    process_single_episode,
                    ep_num,
                    main_torrent_file,
                    main_file_map,
                    backup_maps,
                    folder_id,
                    anime_id,
                    title,
                    collection_name,
                    db
                )
                futures[f] = ep_num

            for future in as_completed(futures):
                ep = futures[future]
                try:
                    res = future.result()
                    if res.get('status') == 'success':
                        batch_uploaded_count += 1
                        if ep > ram_tracker:
                            ram_tracker = ep
                            series_ref.update({'last_uploaded_ep': ram_tracker})
                    else:
                        batch_failed_eps.append(ep)
                except Exception as e:
                    log(f"❌ Exception processing ep {ep}: {e}")
                    batch_failed_eps.append(ep)

        # Update RTDB live status after chunk completes
        try:
            get_rtdb_ref(f"{RTDB_LONG_NODE}/{anime_id}").update({
                "current_ep": ram_tracker,
                "total_uploaded": ram_tracker,
                "uploaded_in_batch": batch_uploaded_count,
                "failed_episodes": batch_failed_eps,
                "updated_at": int(time.time() * 1000)
            })
        except Exception:
            pass

    # Batch Complete Report
    failed_count = len(batch_failed_eps)
    next_needed = ram_tracker + 1
    final_status = 'completed' if (ram_tracker >= total_eps and failed_count == 0) else 'awaiting_admin_batch'

    series_ref.update({
        'status': final_status,
        'last_uploaded_ep': ram_tracker,
        'next_episode': next_needed,
        'batch_report': {
            'uploaded_in_batch': batch_uploaded_count,
            'total_uploaded': ram_tracker,
            'failed_count': failed_count,
            'failed_episodes': batch_failed_eps,
            'next_episode': next_needed,
            'total_episodes': total_eps,
            'reported_at': firestore.SERVER_TIMESTAMP
        }
    })

    try:
        get_rtdb_ref(f"{RTDB_LONG_NODE}/{anime_id}").set({
            "status": "batch_completed" if failed_count == 0 else "awaiting_next_magnet",
            "anime_id": int(anime_id) if str(anime_id).isdigit() else anime_id,
            "title": title,
            "uploaded_count": batch_uploaded_count,
            "total_uploaded": ram_tracker,
            "failed_count": failed_count,
            "failed_episodes": batch_failed_eps,
            "next_episode": next_needed,
            "total_episodes": total_eps,
            "message": f"Batch finished! Uploaded: {batch_uploaded_count}, Failed: {failed_count}.",
            "updated_at": int(time.time() * 1000)
        })
    except Exception:
        pass

    log(f"🏁 Cloud Mega Batch Finished for {title}! (Uploaded: {batch_uploaded_count}, Failed: {failed_count})")
    return True

# ==========================================
# ⚡ 7. SINGLE EPISODE ORCHESTRATOR
# ==========================================
def execute_cloud_single_episode(db, payload):
    anime_id = payload.get("anime_id")
    ep_num = int(payload.get("ep_num", 1))
    collection_name = payload.get("collection_name", "anime_series")

    series_ref = db.collection(collection_name).document(str(anime_id))
    snap = series_ref.get()
    if not snap.exists:
        log(f"❌ Series document {anime_id} not found!")
        return False

    series_data = snap.to_dict() or {}
    title = series_data.get('title', {}).get('english') or series_data.get('title', {}).get('romaji') or f"Anime {anime_id}"
    folder_id = series_data.get('rpm_folder_id')
    magnet_link = series_data.get('custom_batch_url')
    backup_magnets = [m for m in series_data.get('backup_magnets', []) if m]

    log("==================================================")
    log(f"⚡ Cloud Single Episode Worker: {title} | EPISODE {ep_num}")
    log(f"💬 125-Line Subtitle Scoring & Auto-Translation Pipeline")
    log("==================================================")

    meta_dir = "meta_cache"
    os.makedirs(meta_dir, exist_ok=True)
    main_torrent_file = download_torrent_metadata(magnet_link, work_dir=meta_dir)
    main_file_map = scan_torrent_files(main_torrent_file) if main_torrent_file else {}

    backup_maps = []
    for b_idx, b_mag in enumerate(backup_magnets):
        b_file = download_torrent_metadata(b_mag, work_dir=meta_dir)
        if b_file:
            b_map = scan_torrent_files(b_file)
            if b_map:
                backup_maps.append({'torrent_file': b_file, 'file_map': b_map})

    res = process_single_episode(
        ep_num,
        main_torrent_file,
        main_file_map,
        backup_maps,
        folder_id,
        anime_id,
        title,
        collection_name,
        db
    )

    if res and res.get('status') == 'success':
        log(f"🎉 Episode {ep_num} 100% COMPLETE and uploaded!")
        try:
            total_eps = int(series_data.get('episodes_total', 0))
            eps_snap = series_ref.collection('episodes').where('status', '==', 'uploaded').get()
            uploaded_count = len(eps_snap)
            series_ref.update({
                'last_uploaded_ep': max(ep_num, int(series_data.get('last_uploaded_ep', 0))),
                'uploaded_episodes_count': uploaded_count
            })
            if total_eps > 0 and uploaded_count >= total_eps:
                series_ref.update({'status': 'completed'})
                log(f"🏁 All {total_eps} episodes completed for {title}!")
        except Exception as e:
            log(f"⚠️ Notice on progress update: {e}")
        return True
    else:
        log(f"❌ Episode {ep_num} failed.")
        try:
            ep_doc_id = f"episode_{ep_num:04d}"
            series_ref.collection('episodes').document(ep_doc_id).update({
                'status': 'failed_upload',
                'last_error': 'Failed across available magnets',
                'last_updated': firestore.SERVER_TIMESTAMP
            })
        except Exception:
            pass
        return False

def main():
    log("🤖 Long Anime Cloud Worker Starting...")
    payload_str = os.getenv("JOB_PAYLOAD", "")
    if not payload_str and len(sys.argv) > 1:
        payload_str = sys.argv[1]

    if not payload_str:
        log("❌ No JOB_PAYLOAD provided. Exiting.")
        sys.exit(1)

    try:
        payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
    except Exception as e:
        log(f"❌ Error parsing payload: {e}")
        sys.exit(1)

    anime_id = payload.get("anime_id")
    collection_name = payload.get("collection_name", "anime_series")

    if not anime_id:
        log("❌ No anime_id in payload.")
        sys.exit(1)

    db = init_firebase()

    ep_num = payload.get("ep_num")
    job_type = payload.get("job_type")

    if ep_num is not None or job_type == "single_episode":
        success = execute_cloud_single_episode(db, payload)
    else:
        success = execute_cloud_mega_batch(db, anime_id, collection_name=collection_name)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
