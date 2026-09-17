"""
Universal Subtitle Engine
-------------------------
High-performance, guaranteed Sinhala translation pipeline with:
- Chrome Extension API fallback
- Strict Sinhala character verification (has_sinhala_characters)
- 15 multi-threaded workers
- Ad blocking & dialogue cleaning
- Spoken Sinhala dictionary integration
- 125-line crack / incomplete filter threshold
- Realtime missing subtitle alert dispatcher for Admin Panel
- Direct GitHub Releases Upload (DDL) with exact asset naming (Sinhala.srt / English.srt)
- Auto GitHub Repository Rotation on storage/upload limit + Folder-specific .env rewriting
"""

import os
import io
import re
import sys
import time
import uuid
import random
import threading
import requests
import pysubs2
import chardet
import concurrent.futures
from deep_translator import GoogleTranslator
from dotenv import load_dotenv

# Spoken Sinhala dictionary loading
try:
    from spoken_dict import SPOKEN_DICT
except ImportError:
    try:
        from uploader.spoken_dict import SPOKEN_DICT
    except ImportError:
        try:
            from sub.spoken_dict import SPOKEN_DICT
        except ImportError:
            SPOKEN_DICT = {}

MIN_SUB_LINE_THRESHOLD = 125
MAX_SUB_SIZE = 10 * 1024 * 1024

BAD_WORDS = [
    'subtitle by', 'translated by', 'sync by', 'encoded by', 'www.', '.com',
    'discord', 'telegram', 'netlify', 'anishift', 'download කිරීමට', 'නැරඹීමට',
    'subtitles by', 'opensubtitles', 'subscene'
]

_github_lock = threading.Lock()

def apply_spoken_sinhala(text):
    if not text or not SPOKEN_DICT: return text
    sorted_keys = sorted(SPOKEN_DICT.keys(), key=len, reverse=True)
    result_text = str(text)
    for key in sorted_keys:
        value = SPOKEN_DICT[key]
        pattern = r'(?<![\w\u0D80-\u0DFF])' + re.escape(key) + r'(?![\w\u0D80-\u0DFF])'
        result_text = re.sub(pattern, value, result_text)
    return result_text

def has_sinhala_characters(text):
    return bool(re.search(r'[\u0D80-\u0DFF]', str(text)))

def has_letters(text):
    return bool(re.search(r'[a-zA-Z\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]', str(text)))

def clean_vtt_tags(text):
    if not text: return ""
    text = re.sub(r'\{.*?\}', '', text).replace('\\h', ' ')
    return re.sub(r'<[^>]+>', '', text).strip()

def is_garbage_sub(text):
    if not text: return True
    if re.search(r'\\pos\(|\\c&H|\\alpha|\\t\(|\\fad\(|\\an\d', text): return True
    cl = re.sub(r'<[^>]+>', '', re.sub(r'\{.*?\}', '', text)).strip()
    if re.match(r'^m\s+-?\d+(?:\.\d+)?\s+-?\d+(?:\.\d+)?\s+(?:l|b|s|c|m)\s+', cl): return True
    return False

def detect_encoding(file_path):
    for enc in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                f.read(1024)
            return enc
        except Exception:
            continue
    try:
        with open(file_path, 'rb') as f:
            return chardet.detect(f.read(20000))['encoding'] or 'utf-8'
    except Exception:
        return 'utf-8'

def is_valid_sub_file(file_path):
    try:
        if not os.path.exists(file_path): return False
        size = os.path.getsize(file_path)
        if size < 100 or size > MAX_SUB_SIZE: return False
        with open(file_path, 'rb') as f:
            start = f.read(1024)
            return b"<!DOCTYPE html>" not in start and b"<html" not in start
    except Exception: return False

# 🛡️ Dynamic Proxy Pool Loader
PROXY_POOL = []
_LAST_PROXY_LOAD = 0

def get_proxy_pool():
    global PROXY_POOL, _LAST_PROXY_LOAD
    now = time.time()
    if now - _LAST_PROXY_LOAD < 30 and PROXY_POOL:
        return PROXY_POOL

    pool = []
    search_paths = [
        "proxies.txt",
        os.path.join("..", "proxies.txt"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.txt"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "proxies.txt")
    ]
    for sp in search_paths:
        if os.path.exists(sp):
            try:
                with open(sp, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"): continue
                        parts = line.split(":")
                        if len(parts) == 4:
                            ip, port, u, p = parts
                            pool.append(f"http://{u}:{p}@{ip}:{port}")
                        elif len(parts) == 2:
                            pool.append(f"http://{line}")
                        elif "://" in line:
                            pool.append(line)
                if pool:
                    break
            except Exception:
                pass
    PROXY_POOL = pool
    _LAST_PROXY_LOAD = now
    return PROXY_POOL

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
]

def translate_guaranteed_sinhala(text, proxies=None):
    """
    Multi-tier guaranteed Sinhala translation with Proxy Rotation & Automatic Direct Fallback:
    1. Chrome Extension Translate API (clients5.google.com) with Proxy/Direct rotation
    2. GoogleTranslator with Proxy/Direct
    3. Guaranteed Direct Fallback if proxy limits are exceeded or proxies fail
    """
    if not text or len(text.strip()) == 0: return ""
    if not has_letters(text): return text

    active_proxies = proxies
    pool = get_proxy_pool()
    if not active_proxies and pool:
        px = random.choice(pool)
        active_proxies = {"http": px, "https": px}

    ua = random.choice(USER_AGENTS)

    # Strategy 1: Google Chrome Extension API (clients5.google.com) - Fastest & Highest Success
    for attempt in range(2):
        try:
            curr_p = active_proxies if attempt == 0 else None
            url = "https://clients5.google.com/translate_a/t"
            params = {"client": "dict-chrome-ex", "sl": "auto", "tl": "si", "q": text}
            headers = {"User-Agent": ua}
            resp = requests.get(url, params=params, headers=headers, proxies=curr_p, timeout=4)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    res_text = str(data[0][0]) if isinstance(data[0], list) else str(data[0])
                    if res_text and has_sinhala_characters(res_text):
                        return apply_spoken_sinhala(res_text)
        except Exception:
            pass

    # Strategy 2: GoogleTranslator (deep_translator)
    for attempt in range(2):
        try:
            curr_p = active_proxies if attempt == 0 else None
            translator = GoogleTranslator(source='auto', target='si', proxies=curr_p)
            res = translator.translate(text)
            if res and has_sinhala_characters(res):
                return apply_spoken_sinhala(res)
        except Exception:
            time.sleep(0.2)

    # Strategy 3: Direct Chrome API without proxy (Guaranteed Fallback if proxy limit is reached)
    try:
        url = "https://clients5.google.com/translate_a/t"
        params = {"client": "dict-chrome-ex", "sl": "auto", "tl": "si", "q": text}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(url, params=params, headers=headers, timeout=4)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                res_text = str(data[0][0]) if isinstance(data[0], list) else str(data[0])
                if res_text and has_sinhala_characters(res_text):
                    return apply_spoken_sinhala(res_text)
    except Exception:
        pass

    return ""

def clean_sub_events(subs):
    """
    පිරිසිදු dialogues තෝරා ගැනීම සහ duplicate/ad/watermark ඉවත් කිරීම
    """
    cleaned_events = []
    unique_texts = set()
    prev_text = ""
    seen_texts_count = {}

    for e in subs:
        if is_garbage_sub(e.text): continue
        txt = clean_vtt_tags(e.text)
        t_low = txt.lower()

        if any(x in t_low for x in BAD_WORDS) or len(txt) > 250 or len(txt) < 2 or '♪' in txt or '♫' in txt:
            continue

        if txt == prev_text:
            if cleaned_events:
                cleaned_events[-1].end = max(cleaned_events[-1].end, e.end)
            continue

        seen_texts_count[txt] = seen_texts_count.get(txt, 0) + 1
        if len(txt) > 30 and seen_texts_count[txt] > 2:
            continue

        e.text = txt
        cleaned_events.append(e)
        unique_texts.add(txt)
        prev_text = txt

    return cleaned_events, unique_texts

def process_sinhala_sub(sub_path, out_name=None, anime_dict=None, max_workers=5, log_prefix="[SUB-ENGINE]"):
    """
    Complete High-Speed Guaranteed Sinhala Subtitle Processing:
    Loads file -> Cleans dialogues -> Parallel translate (VPS Safe) -> Checks Sinhala characters -> Spoken Dict -> Saves SRT
    """
    max_workers = min(int(max_workers or 5), 5)
    if not is_valid_sub_file(sub_path):
        print(f"{log_prefix} ⚠️ Invalid subtitle file: {sub_path}", flush=True)
        return None

    if not out_name:
        out_name = f"sinhala_sub_{int(time.time())}_{os.getpid()}_{uuid.uuid4().hex[:4]}.srt"

    try:
        enc = detect_encoding(sub_path)
        try:
            subs = pysubs2.load(sub_path, encoding=enc)
        except Exception:
            subs = pysubs2.load(sub_path, encoding='latin-1')

        # 125 Line threshold validation
        if len(subs.events) < MIN_SUB_LINE_THRESHOLD:
            print(f"{log_prefix} 🛑 Rejected sub track: Only {len(subs.events)} lines (Required >= {MIN_SUB_LINE_THRESHOLD})", flush=True)
            return None

        cleaned_events, unique_texts = clean_sub_events(subs)
        if not cleaned_events:
            print(f"{log_prefix} ⚠️ No clean dialogs remaining after filtering.", flush=True)
            return None

        uni_list = list(unique_texts)
        total_lines = len(uni_list)
        print(f"{log_prefix} 🚀 Fast Guaranteed Translation of {total_lines} lines (Workers: {max_workers})...", flush=True)

        translation_map = {}
        def translate_item(text):
            return text, translate_guaranteed_sinhala(text)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(translate_item, t) for t in uni_list]
            done = 0
            for future in concurrent.futures.as_completed(futures):
                orig, trans = future.result()
                if trans and anime_dict:
                    for k, v in anime_dict.items():
                        trans = trans.replace(k, v)
                translation_map[orig] = trans
                done += 1
                if done % 10 == 0 or done == total_lines:
                    print(f"{log_prefix}    📊 Translation Progress: {int((done / total_lines) * 100)}% ({done}/{total_lines} lines)", flush=True)

        final_events = []
        for event in cleaned_events:
            translated_text = translation_map.get(event.text, "")
            if translated_text:
                event.text = translated_text
                final_events.append(event)

        subs.events = final_events
        subs.save(out_name, encoding="utf-8")
        print(f"{log_prefix} ✅ Sinhala Subtitle Generated Successfully: {out_name}", flush=True)
        return out_name

    except Exception as e:
        print(f"{log_prefix} ❌ Error in process_sinhala_sub: {e}", flush=True)
        return None

def process_english_sub(sub_path, out_name=None, log_prefix="[SUB-ENGINE]"):
    """
    Clean & Format English Subtitle Track with 125-Line Threshold Filter
    """
    if not is_valid_sub_file(sub_path):
        return None
    try:
        enc = detect_encoding(sub_path)
        try:
            subs = pysubs2.load(sub_path, encoding=enc)
        except Exception:
            subs = pysubs2.load(sub_path, encoding='latin-1')

        if len(subs.events) < MIN_SUB_LINE_THRESHOLD:
            print(f"{log_prefix} 🛑 Rejected English sub: Only {len(subs.events)} lines (< {MIN_SUB_LINE_THRESHOLD})", flush=True)
            return None

        cleaned_events, _ = clean_sub_events(subs)
        if not cleaned_events:
            return None

        subs.events = cleaned_events
        if not out_name:
            out_name = f"english_sub_{int(time.time())}_{os.getpid()}_{uuid.uuid4().hex[:4]}.srt"
        subs.save(out_name, encoding="utf-8")
        return out_name
    except Exception as e:
        print(f"{log_prefix} ❌ Error in process_english_sub: {e}", flush=True)
        return None

# =====================================================================
# 🔔 MISSING SUBTITLE ALERT DISPATCHER (FOR ADMIN PANEL)
# =====================================================================

def push_missing_sub_alert(rtdb_module, anime_id, anime_title, episode_number, video_id=None, server=1):
    """
    RPMShare එකෙන් subtitle එකක් හමුනොවුනහොත් Admin Panel එකේ
    Custom Subs section එකට Alert Card එකක් ලියයි. (Disabled per request)
    """
    return

def clear_missing_sub_alert(rtdb_module, anime_id, episode_number=None):
    """
    Sub එක successfully attach වූ විට RTDB alert එක ඉවත් කරයි.
    """
    try:
        if episode_number is not None:
            alert_key = f"{anime_id}_ep_{episode_number}"
            rtdb_module.reference('missing_sub_alerts').child(alert_key).delete()
            print(f"✅ Cleared missing sub alert: {alert_key}", flush=True)
        else:
            alerts = rtdb_module.reference('missing_sub_alerts').get() or {}
            for k, val in alerts.items():
                if isinstance(val, dict) and val.get('anilist_id') == int(anime_id):
                    rtdb_module.reference('missing_sub_alerts').child(k).delete()
    except Exception as e:
        print(f"⚠️ Error clearing missing sub alert: {e}", flush=True)

# =====================================================================
# 🚀 GITHUB RELEASES DDL UPLOADER & AUTO-REPO ROTATION
# =====================================================================

def get_env_file_for_caller(caller_env=None):
    """
    Finds the specific .env file corresponding to the executing script / module.
    Each bot folder (uploader/, long_uploader/, sub/, or root) has its own .env.
    """
    if caller_env:
        if os.path.isfile(caller_env):
            return os.path.abspath(caller_env)
        if os.path.isdir(caller_env):
            p = os.path.join(caller_env, ".env")
            if os.path.exists(p): return os.path.abspath(p)

    # Check caller script directory
    if sys.argv and sys.argv[0]:
        caller_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        candidate = os.path.join(caller_dir, ".env")
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

    # Check current working directory
    if os.path.exists(".env"):
        return os.path.abspath(".env")
    return None

def update_env_repo(new_repo_full, env_file_path=None):
    """
    Rewrites GITHUB_REPO in the targeted .env file and updates os.environ.
    """
    if not env_file_path:
        env_file_path = get_env_file_for_caller()

    if env_file_path and os.path.exists(env_file_path):
        try:
            with open(env_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            found = False
            with open(env_file_path, "w", encoding="utf-8") as f:
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith("GITHUB_REPO=") or stripped.startswith("GITHUB_REPO ="):
                        f.write(f'GITHUB_REPO="{new_repo_full}"\n')
                        found = True
                    else:
                        f.write(line)
                if not found:
                    f.write(f'\nGITHUB_REPO="{new_repo_full}"\n')
            print(f"[SUB-ENGINE] 📝 Auto-updated {env_file_path} with new Repo: {new_repo_full}", flush=True)
        except Exception as e:
            print(f"[SUB-ENGINE] ⚠️ Failed to update .env ({env_file_path}): {e}", flush=True)

    os.environ["GITHUB_REPO"] = new_repo_full

def get_github_credentials(caller_env=None):
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")
    if not token or not repo:
        env_f = get_env_file_for_caller(caller_env)
        if env_f and os.path.exists(env_f):
            load_dotenv(env_f, override=False)
            token = token or os.getenv("GITHUB_TOKEN")
            repo = repo or os.getenv("GITHUB_REPO")
    # Check Firebase RTDB for auto-rotated active repository
    try:
        from firebase_admin import db as rtdb
        active_vault = rtdb.reference("config/active_sub_vault").get()
        if active_vault and isinstance(active_vault, str) and "/" in active_vault:
            repo = active_vault.strip('"\'').strip()
    except Exception:
        pass

    token = token or os.getenv("GITHUB_TOKEN", "")
    repo = repo or os.getenv("GITHUB_REPO", "Anishift-svr/sub-vault-160633")
    if token: token = token.strip('"\'').strip()
    if repo: repo = repo.strip('"\'').strip()
    return token, repo

def create_new_github_repo(token, caller_env=None):
    """
    Auto-create a new repository on GitHub when storage/upload limit is reached
    and rewrites the caller folder's .env file.
    """
    with _github_lock:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        username = "Anishift-svr"
        try:
            u_res = requests.get("https://api.github.com/user", headers=headers, timeout=10)
            if u_res.status_code == 200:
                username = u_res.json().get("login", username)
        except Exception: pass

        new_repo_name = f"sub-vault-{uuid.uuid4().hex[:6]}"
        print(f"[SUB-ENGINE] ⚙️ Limit reached. Creating NEW GitHub Repository: {new_repo_name}...", flush=True)
        repo_data = {"name": new_repo_name, "private": False, "auto_init": True}
        r = requests.post("https://api.github.com/user/repos", headers=headers, json=repo_data, timeout=20)
        if r.status_code in [200, 201]:
            new_full_repo = f"{username}/{new_repo_name}"
            env_file = get_env_file_for_caller(caller_env)
            update_env_repo(new_full_repo, env_file)
            try:
                from firebase_admin import db as rtdb
                rtdb.reference("config/active_sub_vault").set(new_full_repo)
                print(f"[SUB-ENGINE] 🌐 Synced new repo '{new_full_repo}' to Firebase RTDB config/active_sub_vault", flush=True)
            except Exception:
                pass
            time.sleep(2)
            return new_full_repo
        else:
            print(f"[SUB-ENGINE] ❌ Failed to create new GitHub repo: {r.status_code} - {r.text}", flush=True)
            return None

def upload_to_github_release(file_path, asset_name="Sinhala.srt", release_context=None, caller_env=None, max_retries=2):
    """
    Upload subtitle file to GitHub Releases as a Direct Download Link (DDL).
    Ensures filename in download URL is strictly asset_name ('Sinhala.srt' or 'English.srt').
    
    Parameters:
    - file_path: local path to the subtitle file (.srt)
    - asset_name: exact filename in GitHub release (e.g. 'Sinhala.srt' or 'English.srt')
    - release_context: shared dict across episode tracks to group Sinhala and English into the same release.
    - caller_env: optional file or directory path to locate the bot's specific .env.
    - max_retries: retry attempts on failure.
    
    Returns:
    - Direct browser download URL ending in /Sinhala.srt or /English.srt, or None.
    """
    if not file_path or not os.path.exists(file_path):
        return None
    try:
        if os.path.getsize(file_path) == 0:
            print(f"[SUB-ENGINE] ⚠️ Subtitle file is empty: {file_path}", flush=True)
            return None
    except Exception: return None

    token, repo = get_github_credentials(caller_env)

    for attempt in range(max_retries):
        try:
            release_id = None
            upload_url_base = None

            # Check if release_context already contains a valid release on the current repo
            if release_context is not None and isinstance(release_context, dict):
                if release_context.get("repo") == repo:
                    release_id = release_context.get("release_id")
                    upload_url_base = release_context.get("upload_url")
                else:
                    release_context.clear()

            # Create a dedicated release tag for this episode if not already existing
            if not release_id or not upload_url_base:
                tag_name = f"sub-{uuid.uuid4().hex[:8]}"
                rel_api = f"https://api.github.com/repos/{repo}/releases"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github.v3+json",
                    "User-Agent": "Anishift-SubEngine"
                }
                rel_payload = {
                    "tag_name": tag_name,
                    "name": f"Subtitle Storage {tag_name}",
                    "draft": False,
                    "prerelease": False
                }
                r_rel = requests.post(rel_api, headers=headers, json=rel_payload, timeout=20)
                if r_rel.status_code in [200, 201]:
                    rel_json = r_rel.json()
                    release_id = rel_json.get("id")
                    raw_upload = rel_json.get("upload_url", "")
                    upload_url_base = raw_upload.split("{")[0] if "{" in raw_upload else raw_upload
                    if release_context is not None and isinstance(release_context, dict):
                        release_context["release_id"] = release_id
                        release_context["upload_url"] = upload_url_base
                        release_context["repo"] = repo
                        release_context["tag"] = tag_name
                else:
                    print(f"[SUB-ENGINE] ⚠️ Release creation failed ({r_rel.status_code}) on repo {repo}. Rotating repo...", flush=True)
                    new_repo = create_new_github_repo(token, caller_env)
                    if new_repo:
                        repo = new_repo
                    continue

            # Upload asset with exact asset_name (e.g. Sinhala.srt or English.srt)
            upload_target = f"{upload_url_base}?name={asset_name}"
            upload_headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "text/plain; charset=utf-8",
                "User-Agent": "Anishift-SubEngine"
            }
            with open(file_path, "rb") as f:
                r_up = requests.post(upload_target, headers=upload_headers, data=f, timeout=60)

            if r_up.status_code == 201:
                dl_url = r_up.json().get("browser_download_url")
                print(f"[SUB-ENGINE] ✅ GitHub DDL Upload Success [{asset_name}]: {dl_url}", flush=True)
                return dl_url
            elif r_up.status_code == 422 and "already_exists" in r_up.text:
                if release_context is not None and isinstance(release_context, dict):
                    release_context.pop("release_id", None)
                    release_context.pop("upload_url", None)
                continue
            else:
                print(f"[SUB-ENGINE] ⚠️ Asset upload failed ({r_up.status_code}): {r_up.text}. Rotating repo...", flush=True)
                new_repo = create_new_github_repo(token, caller_env)
                if new_repo:
                    repo = new_repo
                    if release_context is not None and isinstance(release_context, dict):
                        release_context.pop("release_id", None)
                        release_context.pop("upload_url", None)
                continue

        except Exception as e:
            print(f"[SUB-ENGINE] ❌ GitHub upload error: {e}", flush=True)
            new_repo = create_new_github_repo(token, caller_env)
            if new_repo:
                repo = new_repo
                if release_context is not None and isinstance(release_context, dict):
                    release_context.pop("release_id", None)
                    release_context.pop("upload_url", None)

    return None

def check_rpm_video_status(video_id, api_token=None, base_url="https://rpmshare.com/api/v1"):
    """
    Checks the status of a video on RPM ('Active', 'Pending', 'Processing', etc.).
    """
    if not video_id:
        return None
    if not api_token:
        api_token = os.getenv("RPMSHARE_API_TOKEN")
    headers = {'api-token': api_token}
    try:
        r = requests.get(f"{base_url}/video/manage/{video_id}", headers=headers, timeout=15)
        if r.status_code == 200:
            return r.json().get('status')
    except Exception:
        pass
    return None

def delete_existing_sinhala_subs(video_id, api_token=None, base_url="https://rpmshare.com/api/v1", log_prefix="SUB-ENGINE"):
    """
    Deletes any existing Sinhala subtitle track on RPM to avoid duplicate or conflicting tracks.
    """
    if not video_id:
        return
    if not api_token:
        api_token = os.getenv("RPMSHARE_API_TOKEN")
    headers = {'api-token': api_token}
    try:
        resp = requests.get(f"{base_url}/video/manage/{video_id}/files", headers=headers, timeout=15)
        if resp.status_code == 200:
            files = resp.json()
            for f in files:
                if f.get('type') == 'Subtitle':
                    lang = (f.get('language') or '').lower()
                    name = (f.get('name') or '').lower()
                    if lang == 'si' or 'si' in name or 'sinhala' in name or 'සිංහල' in name:
                        sub_id = f.get('id')
                        if sub_id:
                            print(f"[{log_prefix}] 🗑️ Deleting old Sinhala sub (ID: {sub_id}) from RPM Video {video_id}...", flush=True)
                            requests.delete(f"{base_url}/video/manage/{video_id}/subtitle/{sub_id}", headers=headers, timeout=10)
    except Exception as e:
        print(f"[{log_prefix}] ⚠️ Error checking/deleting old subs on RPM: {e}", flush=True)

def _perform_rpm_sub_attach(video_id, sub_bytes, sub_filename, api_token, remote_url=None, base_url="https://rpmshare.com/api/v1", log_prefix="SUB-ENGINE"):
    """
    Attempts to attach the subtitle using direct file upload (PUT) first,
    and falls back to remote-subtitle (POST) if remote_url is provided.
    """
    headers = {'api-token': api_token}
    
    # Clean previous Sinhala tracks first
    delete_existing_sinhala_subs(video_id, api_token=api_token, base_url=base_url, log_prefix=log_prefix)

    # 1. Try PUT /subtitle if sub_bytes are available
    if sub_bytes:
        for attempt in range(2):
            try:
                url = f"{base_url}/video/manage/{video_id}/subtitle"
                files = {'file': (sub_filename or 'Sinhala.srt', io.BytesIO(sub_bytes), 'application/x-subrip')}
                data = {'language': 'si', 'name': 'සිංහල'}
                r = requests.put(url, headers=headers, files=files, data=data, timeout=40)
                if r.status_code in [200, 201]:
                    return True, r.text
                else:
                    print(f"[{log_prefix}] ⚠️ PUT /subtitle returned status {r.status_code}: {r.text[:100]}", flush=True)
            except Exception as e:
                print(f"[{log_prefix}] ⚠️ PUT /subtitle error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2)

    # 2. Fallback to POST /remote-subtitle if remote_url (e.g. GitHub Releases DDL) is provided
    if remote_url:
        for attempt in range(2):
            try:
                url_remote = f"{base_url}/video/manage/{video_id}/remote-subtitle"
                headers_json = {'api-token': api_token, 'Content-Type': 'application/json'}
                payload = {
                    'language': 'si',
                    'name': 'සිංහල',
                    'url': remote_url,
                    'type': 'srt'
                }
                r_rem = requests.post(url_remote, headers=headers_json, json=payload, timeout=40)
                if r_rem.status_code in [200, 201]:
                    return True, r_rem.text
                else:
                    print(f"[{log_prefix}] ⚠️ POST /remote-subtitle returned status {r_rem.status_code}: {r_rem.text[:100]}", flush=True)
            except Exception as e:
                print(f"[{log_prefix}] ⚠️ POST /remote-subtitle error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2)

    return False, None

def upload_sub_to_rpm(video_id, sub_file=None, api_token=None, remote_url=None, base_url="https://rpmshare.com/api/v1", background_if_pending=True, log_prefix="SUB-ENGINE"):
    """
    Uploads/attaches a Sinhala subtitle track ('සිංහල') to an RPM video.
    If the video is still transcoding (status == 'Pending' or 'Processing'),
    it launches a background daemon worker to automatically poll and attach
    the subtitle the moment the video transitions to 'Active'.
    This prevents RPM 400 Bad Request ("Upload subtitle failed!") and does not block the bot.
    """
    if not video_id:
        return False
    if not api_token:
        api_token = os.getenv("RPMSHARE_API_TOKEN")

    sub_bytes = None
    sub_filename = 'Sinhala.srt'
    if sub_file:
        if isinstance(sub_file, (bytes, bytearray)):
            sub_bytes = bytes(sub_file)
        elif isinstance(sub_file, str) and os.path.exists(sub_file):
            try:
                sub_filename = os.path.basename(sub_file)
                with open(sub_file, 'rb') as sf:
                    sub_bytes = sf.read()
            except Exception as e:
                print(f"[{log_prefix}] ⚠️ Error reading sub_file into memory: {e}", flush=True)

    if not sub_bytes and not remote_url:
        print(f"[{log_prefix}] ⚠️ upload_sub_to_rpm called with neither valid sub_file nor remote_url!", flush=True)
        return False

    status = check_rpm_video_status(video_id, api_token=api_token, base_url=base_url)

    # 1. If video is already Active on RPM, attach immediately!
    if status == 'Active':
        ok, _ = _perform_rpm_sub_attach(video_id, sub_bytes, sub_filename, api_token, remote_url=remote_url, base_url=base_url, log_prefix=log_prefix)
        if ok:
            print(f"[{log_prefix}] 🎬 ✅ Attached Sinhala Subtitle to RPM Player: Video {video_id}", flush=True)
            return True

    # 2. If video is still transcoding (Pending / Processing), queue background worker
    if background_if_pending:
        def _bg_worker(v_id, s_bytes, s_name, tok, r_url, b_url, l_prefix, init_status):
            print(f"[{l_prefix}] ⏳ Video {v_id} is '{init_status or 'Pending'}' (Transcoding). Background worker waiting for 'Active'...", flush=True)
            for attempt in range(1, 91):  # Poll every 20s up to 30 minutes
                time.sleep(20)
                st = check_rpm_video_status(v_id, api_token=tok, base_url=b_url)
                if st == 'Active':
                    print(f"[{l_prefix}] ⚡ Video {v_id} is now 'Active'! Attaching Sinhala subtitle...", flush=True)
                    ok, _ = _perform_rpm_sub_attach(v_id, s_bytes, s_name, tok, remote_url=r_url, base_url=b_url, log_prefix=l_prefix)
                    if ok:
                        print(f"[{l_prefix}] 🎬 ✅ Successfully attached Sinhala subtitle to RPM Player: Video {v_id}", flush=True)
                        return
                    else:
                        print(f"[{l_prefix}] ⚠️ Failed to attach subtitle to Active Video {v_id}", flush=True)
                        return
                elif st in ['Error', 'Failed', 'Cancelled']:
                    print(f"[{l_prefix}] ❌ Video {v_id} entered state '{st}'. Aborting subtitle attachment.", flush=True)
                    return
            print(f"[{l_prefix}] ⏱️ Timeout waiting for Video {v_id} to become Active on RPM.", flush=True)

        threading.Thread(
            target=_bg_worker,
            args=(video_id, sub_bytes, sub_filename, api_token, remote_url, base_url, log_prefix, status),
            daemon=True
        ).start()
        print(f"[{log_prefix}] 🚀 Queued background subtitle attacher for RPM Video {video_id} (will attach as soon as Active)", flush=True)
        return True
    else:
        print(f"[{log_prefix}] ⏳ Video {video_id} is '{status or 'Pending'}' (Transcoding). Waiting for 'Active' before attaching subtitle...", flush=True)
        for attempt in range(1, 61):  # Poll every 10s up to 10 minutes
            time.sleep(10)
            st = check_rpm_video_status(video_id, api_token=api_token, base_url=base_url)
            if st == 'Active':
                print(f"[{log_prefix}] ⚡ Video {video_id} is now 'Active'! Attaching Sinhala subtitle...", flush=True)
                ok, res_text = _perform_rpm_sub_attach(video_id, sub_bytes, sub_filename, api_token, remote_url=remote_url, base_url=base_url, log_prefix=log_prefix)
                if ok:
                    print(f"[{log_prefix}] 🎬 ✅ Attached Sinhala Subtitle to RPM Player: Video {video_id}", flush=True)
                    return True
                else:
                    print(f"[{log_prefix}] ⚠️ Failed to attach subtitle to Active Video {video_id}: {res_text}", flush=True)
                    return False
            elif st in ['Error', 'Failed', 'Cancelled']:
                print(f"[{log_prefix}] ❌ Video {video_id} entered state '{st}'. Aborting subtitle attachment.", flush=True)
                return False
        print(f"[{log_prefix}] ⏱️ Timeout waiting for Video {video_id} to become Active on RPM.", flush=True)
        return False

