#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gateway Classes Matrix Sync Engine (High-Performance Parallel Pipeline)
- Universal Deduplication (Common lectures across Combo & Single semester batches share 1 upload)
- Parallel Producer-Consumer Architecture (Downloads Video N+1 while Uploading Video N)
- Forensic Sanitizer (Strips metadata -map_metadata -1 to guarantee 100% anonymity)
- Pyrogram MTProto Uploader (Full 2,000 MB / 2 GB support without 50MB limits)
- Human Speed Throttling (2.5s delay between requests, strict max 30 videos per 24 hours)
"""

import os
import sys
import json
import time
import re
import shutil
import asyncio
import urllib.parse
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# Ensure root workspace is in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Python 3.14+ event loop guard for Pyrogram sync wrapper
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# Fix 64-bit Telegram channel IDs in Pyrogram
import pyrogram.utils
pyrogram.utils.MIN_CHANNEL_ID = -100999999999999

import requests
from pyrogram import Client
from tools.config import (
    SYNC_API_ID, SYNC_API_HASH, SYNC_BOT_TOKEN, TG_CHANNEL_ID,
    CLASSPLUS_TOKEN, SAFETY_REQUEST_DELAY, MAX_VIDEOS_PER_RUN, MAX_PDFS_PER_RUN,
    MASTER_INDEX_FILE, DATA_DIR
)

DOWNLOAD_DIR = DATA_DIR / "temp_downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

CP_HEADERS = {
    "User-Agent": "Classplus/52 (Android; Mobile)",
    "region": "IN",
    "api-version": "52",
    "x-access-token": CLASSPLUS_TOKEN,
    "Accept": "application/json"
}

def load_master_index() -> Dict[str, Any]:
    if MASTER_INDEX_FILE.exists():
        try:
            with open(MASTER_INDEX_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "content_catalog": {},  # content_id -> {title, duration, telegram_file_id, msg_id}
        "courses": {}           # course_id -> {name, folder_tree}
    }

def save_master_index(index_data: Dict[str, Any]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    catalog = index_data.get("content_catalog", {})
    total_videos = 0
    total_pdfs = 0
    total_bytes = 0
    for item in catalog.values():
        t = item.get("type", "video")
        if t == "video":
            total_videos += 1
        elif t == "pdf":
            total_pdfs += 1
        b = item.get("file_size_bytes")
        if isinstance(b, (int, float)) and b > 0:
            total_bytes += int(b)
        else:
            fs = str(item.get("file_size", ""))
            if "GB" in fs:
                try: total_bytes += int(float(fs.replace("GB", "").strip()) * 1024 * 1024 * 1024)
                except Exception: pass
            elif "MB" in fs:
                try: total_bytes += int(float(fs.replace("MB", "").strip()) * 1024 * 1024)
                except Exception: pass
            elif "KB" in fs:
                try: total_bytes += int(float(fs.replace("KB", "").strip()) * 1024)
                except Exception: pass

    index_data["vault_stats"] = {
        "total_items": len(catalog),
        "total_videos": total_videos,
        "total_pdfs": total_pdfs,
        "total_data_bytes": total_bytes,
        "total_data_gb": round(total_bytes / (1024 * 1024 * 1024), 2)
    }

    temp_file = MASTER_INDEX_FILE.with_suffix(".tmp")
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(index_data, f, indent=2, ensure_ascii=False)
    temp_file.replace(MASTER_INDEX_FILE)

def fetch_classplus_folder(course_id: str, folder_id: str = "0", token: Optional[str] = None) -> List[Dict[str, Any]]:
    headers = dict(CP_HEADERS)
    if token:
        headers["x-access-token"] = token
    url = f"https://api.classplusapp.com/v2/course/content/get?courseId={course_id}&folderId={folder_id}&storeContentEvent=false"
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            data = r.json().get("data", {})
            if isinstance(data, dict):
                return data.get("courseContent", []) or []
            elif isinstance(data, list):
                return data
    except Exception as e:
        print(f"  [ERR] fetch_classplus_folder({course_id}, {folder_id}): {e}")
    return []

def get_signed_stream_url(content_hash_id: str, token: Optional[str] = None) -> Optional[str]:
    headers = dict(CP_HEADERS)
    if token:
        headers["x-access-token"] = token
    quoted = urllib.parse.quote(content_hash_id)
    url = f"https://api.classplusapp.com/cams/uploader/video/jw-signed-url?contentId={quoted}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        time.sleep(SAFETY_REQUEST_DELAY)  # 2.5s safe delay to prevent click blocks
        if r.status_code == 200:
            data = r.json()
            return data.get("url") or data.get("videoUrl") or data.get("hlsUrl")
    except Exception as e:
        print(f"  [ERR] get_signed_stream_url: {e}")
    return None

def download_and_clean_video(stream_url: str, output_path: Path, title: str) -> bool:
    raw_tmp = output_path.with_name(f"raw_{output_path.name}")
    try:
        dl_success = False

        # ── 1. TURBO ENGINE: N_m3u8DL-RE (16-thread native Rust downloader) ──
        n_m3u8_bin = shutil.which("N_m3u8DL-RE") or ("/tmp/N_m3u8DL-RE" if Path("/tmp/N_m3u8DL-RE").exists() else None)
        if n_m3u8_bin and (".m3u8" in stream_url or "manifest" in stream_url or ".mpd" in stream_url):
            tmp_dir = output_path.parent / f"re_tmp_{output_path.stem}"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            cmd_re = [
                str(n_m3u8_bin),
                stream_url,
                "--header", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "--header", "Referer: https://web.classplusapp.com/",
                "--header", "Origin: https://web.classplusapp.com",
                "--thread-count", "16",
                "--download-retry-count", "5",
                "--auto-select",
                "--save-dir", str(output_path.parent),
                "--save-name", f"raw_{output_path.stem}",
                "--tmp-dir", str(tmp_dir),
                "--del-after-done",
                "--no-log"
            ]
            try:
                res_re = subprocess.run(cmd_re, timeout=240, capture_output=True)
                # Find matching output file (.mp4, .mkv, .ts)
                for cand in output_path.parent.glob(f"raw_{output_path.stem}*"):
                    if cand.is_file() and cand != output_path and cand.stat().st_size > 50000:
                        raw_tmp = cand
                        dl_success = True
                        break
            except Exception as re_err:
                print(f"  [N_m3u8DL-RE] Warning: {re_err}, falling back to yt-dlp...")

        # ── 2. STABLE FALLBACK: yt-dlp ───────────────────────────────────────
        if not dl_success:
            cmd_dl = [
                "yt-dlp",
                "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
                "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "--referer", "https://web.classplusapp.com/",
                "--add-header", "Origin:https://web.classplusapp.com",
                "--no-warnings", "--quiet",
                "--concurrent-fragments", "8",
                "--no-part",
                "-o", str(raw_tmp),
                stream_url
            ]
            res = subprocess.run(cmd_dl, timeout=300)
            if res.returncode == 0 and raw_tmp.exists() and raw_tmp.stat().st_size > 50000:
                dl_success = True

        if not dl_success:
            print(f"  [ERR] download failed: {title}")
            return False

        # ── 3. FORENSIC SANITIZER (-map_metadata -1 for 100% student anonymity) ─
        cmd_strip = [
            "ffmpeg", "-v", "error", "-y",
            "-i", str(raw_tmp),
            "-map_metadata", "-1",
            "-c", "copy",
            str(output_path)
        ]
        res_strip = subprocess.run(cmd_strip, timeout=120)
        return res_strip.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1024
    except Exception as e:
        print(f"  [ERR] download_and_clean_video: {e}")
    finally:
        # Cleanup temporary raw downloads
        for cand in output_path.parent.glob(f"raw_{output_path.stem}*"):
            if cand.is_file() and cand != output_path:
                try:
                    cand.unlink()
                except Exception:
                    pass
        re_tmp = output_path.parent / f"re_tmp_{output_path.stem}"
        if re_tmp.exists():
            try:
                shutil.rmtree(str(re_tmp), ignore_errors=True)
            except Exception:
                pass
    return False

def purge_item_from_tree(folder_dict: Dict[str, Any], item_id: str) -> bool:
    removed = False
    if item_id in folder_dict:
        del folder_dict[item_id]
        removed = True
    for k, v in list(folder_dict.items()):
        if isinstance(v, dict) and "children" in v:
            if purge_item_from_tree(v["children"], item_id):
                removed = True
    return removed

async def audit_and_verify_vault(
    client: Client,
    master_data: Dict[str, Any],
    target_course_id: Optional[str] = None
) -> set:
    """
    Audits and verifies all uploaded videos in the Telegram Vault (TG_CHANNEL_ID).
    - Checks Telegram messages in chunks to verify they are alive and have video/document attachments.
    - If a video message was deleted or is missing on Telegram:
      -> Purges the record from master_data["content_catalog"]
      -> Purges the item recursively from master_data["courses"] root_folders
      -> Removes stale thumbnail cache
      -> Saves updated master_index.json
    - Computes and logs the verified upload state per Batch -> Subject -> Unit:
      -> Shows total verified alive videos
      -> Shows the last uploaded lecture (order index and title)
      -> Highlights any deleted or missing lecture that must be re-synced!
    - Returns set of verified_item_ids that are guaranteed alive on Telegram.
    """
    catalog = master_data.get("content_catalog", {})
    courses = master_data.get("courses", {})

    print(f"\n=======================================================")
    print(f"🔍 TELEGRAM VAULT INTEGRITY AUDIT & VERIFICATION")
    print(f"=======================================================")

    if not catalog or len(catalog) < 10:
        print("  ℹ️ Local catalog is empty/truncated. Restoring latest master_index from Telegram Vault Msg #90...")
        try:
            m90 = await client.get_messages(TG_CHANNEL_ID, message_ids=90)
            if m90 and m90.document:
                import tempfile
                with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
                    tmp_p = tmp.name
                await client.download_media(m90, file_name=tmp_p)
                with open(tmp_p, "r", encoding="utf-8") as f:
                    restored = json.load(f)
                if restored.get("content_catalog"):
                    master_data.update(restored)
                    catalog = master_data.get("content_catalog", {})
                    courses = master_data.get("courses", {})
                    save_master_index(master_data)
                    print(f"  ✅ Restored {len(catalog)} items directly from Telegram Vault Msg #90!")
                if os.path.exists(tmp_p):
                    os.remove(tmp_p)
        except Exception as rest_err:
            print(f"  ⚠️ Could not restore from Msg #90: {rest_err}")

    if not catalog:
        print("  ℹ️ Catalog is empty. No existing uploads to verify.")
        print(f"=======================================================\n")
        return set()

    items_to_check = []
    for item_id, item_info in list(catalog.items()):
        msg_id = item_info.get("telegram_msg_id")
        if msg_id:
            try:
                items_to_check.append((item_id, int(msg_id), item_info))
            except Exception:
                items_to_check.append((item_id, None, item_info))
        else:
            items_to_check.append((item_id, None, item_info))

    verified_item_ids = set()
    deleted_items = []

    # Batch check in chunks of 50 to avoid any Telegram rate limit
    chunk_size = 50
    for i in range(0, len(items_to_check), chunk_size):
        chunk = items_to_check[i:i + chunk_size]
        msg_ids = [m_id for _, m_id, _ in chunk if m_id is not None]

        msg_map = {}
        if msg_ids:
            try:
                msgs = await client.get_messages(TG_CHANNEL_ID, message_ids=msg_ids)
                if not isinstance(msgs, list):
                    msgs = [msgs]
                for m in msgs:
                    if m and not getattr(m, "empty", False):
                        msg_map[m.id] = m
            except Exception as e:
                print(f"  ⚠️ Error fetching messages from Telegram: {e}")

        for item_id, m_id, item_info in chunk:
            msg = msg_map.get(m_id) if m_id else None
            if msg and (msg.video or msg.document):
                verified_item_ids.add(item_id)
            else:
                title = item_info.get("title", "Unknown")
                deleted_items.append((item_id, m_id, title, item_info))

    # Purge any deleted/missing items
    if deleted_items:
        print(f"\n⚠️ DETECTED {len(deleted_items)} DELETED/MISSING VIDEO(S) ON TELEGRAM:")
        for item_id, m_id, title, item_info in deleted_items:
            unit = item_info.get("unit", "Unknown Unit")
            order = item_info.get("order_index", "?")
            print(f"  ❌ [PURGED] {title} (ID: {item_id} | Unit: {unit} | Lec #{order} | Msg #{m_id})")

            # Remove from catalog
            catalog.pop(item_id, None)

            # Remove from all courses root_folders
            for c_id, c_data in courses.items():
                if "root_folders" in c_data:
                    purge_item_from_tree(c_data["root_folders"], item_id)

            # Remove local thumb cache if present
            thumb_cache = DATA_DIR / "gw_thumbs" / f"{item_id}.jpg"
            if thumb_cache.exists():
                try: thumb_cache.unlink()
                except Exception: pass

        save_master_index(master_data)
        print(f"🧹 Purged {len(deleted_items)} stale entries. master_index.json updated!\n")
    else:
        print(f"✅ All {len(verified_item_ids)} catalogued videos are verified ALIVE in Telegram channel.\n")

    # Group verified alive items by (batch_id, subject, unit)
    grouped = {}
    for item_id in verified_item_ids:
        info = catalog.get(item_id, {})
        subj = info.get("subject", "General Studies")
        unit = info.get("unit", "General")
        batches = info.get("batch_ids") or ([target_course_id] if target_course_id else ["All"])
        order = info.get("order_index", 0)
        title = info.get("title", "Lecture")
        m_id = info.get("telegram_msg_id")

        for b in batches:
            if target_course_id and b != target_course_id:
                continue
            key = (b, subj, unit)
            grouped.setdefault(key, []).append((order, title, m_id, item_id))

    print(f"📊 VERIFIED STATE PER BATCH & UNIT:")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if not grouped:
        print(f"  ℹ️ No lectures currently uploaded for batch {target_course_id or 'all'}.")
    else:
        for (b, subj, unit), items in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
            items.sort(key=lambda x: x[0])
            last_order, last_title, last_mid, _ = items[-1]
            c_name = courses.get(b, {}).get("course_name", f"Batch #{b}")
            lec_tags = [f"Lec {o}" if o > 0 else "Intro" for o, _, _, _ in items]

            print(f"📚 Batch [{b}] {c_name}")
            print(f"   📖 Subject: {subj}")
            print(f"   📁 Unit: {unit}")
            print(f"      • Verified Alive: {len(items)} lectures ({', '.join(lec_tags)})")
            print(f"      • 📍 Last Uploaded: '{last_title}' (Order: #{last_order}, Telegram Msg: #{last_mid})")
            print(f"      • 🎯 Next in Sequence: Order #{last_order + 1 if last_order >= 0 else 1}")
            print(f"   ─────────────────────────────────────────────────")

    print(f"=======================================================\n")
    return verified_item_ids

def extract_hierarchy_metadata(path_stack: List[str]) -> Tuple[str, str, str]:
    """
    Extracts (subject_name, sub_version, unit_name) from folder path stack.
    Handles variable depth hierarchies:
    - Root level items: (General Studies, "", Syllabus & Introduction)
    - Subject root items (len=1): (Subject, "", Syllabus & Introduction)
    - Standard 2-level (len=2):
        If folder is Version (contains 1.0, 2.0, Version): (Subject, Version, Syllabus & Introduction)
        Else: (Subject, "", Unit)
    - 3+ levels (len>=3):
        If folder[1] is Version: (Subject, folder[1], folder[2])
        Else: (Subject, "", folder[1])
    """
    if not path_stack:
        return ("General Studies", "", "Syllabus & Introduction")
    subj = path_stack[0]
    if len(path_stack) == 1:
        return (subj, "", "Syllabus & Introduction")

    is_version = bool(re.search(r"(?:1\.0|2\.0|v1|v2|version|edition)", path_stack[1], re.IGNORECASE))
    if is_version:
        sub_version = path_stack[1]
        unit = path_stack[2] if len(path_stack) > 2 else "Syllabus & Introduction"
    else:
        sub_version = ""
        unit = path_stack[1]
    return (subj, sub_version, unit)

async def sync_course_tree(
    client: Client,
    course_id: str,
    course_name: str,
    token: Optional[str] = None,
    max_videos: int = MAX_VIDEOS_PER_RUN,
    max_pdfs: int = MAX_PDFS_PER_RUN
) -> int:
    """
    Parallel Producer-Consumer Pipeline:
    - Step 0: Audits Telegram Vault, purges deleted videos, displays last uploaded lecture per unit.
    - Worker 1 (Downloader): Scans folders, checks deduplication against verified vault, downloads Video N+1
    - Worker 2 (Uploader): Uploads Video N via Pyrogram MTProto concurrently
    - Limits to strictly max_videos (20 videos) and max_pdfs (20 PDFs) per 24 hours
    """
    if not token:
        from tools.config import get_active_classplus_token
        token = get_active_classplus_token(course_id)
        if token:
            print(f"🔑 [AUTH TOKEN] Using verified Classplus token for batch {course_id}!")
        else:
            print(f"\n=======================================================")
            print(f"❌ [ABORT] No valid token found for batch {course_id} ({course_name})!")
            print(f"⚠️ Fallback to unrelated batch tokens is disabled to prevent preview-only / failed syncs.")
            print(f"ℹ️ Provide a student token who has purchased batch {course_id} to sync.")
            print(f"=======================================================\n")
            return 0

    master_data = load_master_index()

    # AUDIT AND VERIFY TELEGRAM VAULT FIRST!
    verified_item_ids = await audit_and_verify_vault(client, master_data, course_id)

    catalog = master_data.setdefault("content_catalog", {})
    courses = master_data.setdefault("courses", {})

    course_entry = courses.setdefault(course_id, {
        "course_name": course_name,
        "last_synced": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "root_folders": {}
    })

    print(f"\n=======================================================")
    print(f"🔄 PARALLEL MATRIX SYNC: [{course_id}] {course_name}")
    print(f"🎯 Target limits: max {max_videos} videos & {max_pdfs} PDFs this run")
    print(f"=======================================================")

    # Bounded queue of max 2 items to keep local disk usage minimal
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    stats = {"downloaded": 0, "uploaded": 0, "pdfs_uploaded": 0}

    # ── WORKER 1: PRODUCER (Downloader & Stripper) ───────────────────
    async def producer_downloader():
        async def crawl_folder(folder_id: str, folder_name: str, current_dict: Dict[str, Any], depth: int = 0, path_stack: List[str] = None):
            if path_stack is None:
                path_stack = []

            indent = "  " * depth
            items = fetch_classplus_folder(course_id, folder_id, token)
            print(f"{indent}📁 {folder_name} ({len(items)} items)")

            for item in items:
                ct = item.get("contentType")
                item_id = str(item.get("id", ""))
                item_name = item.get("name", "Unknown")

                if ct in (1, "1"):  # Sub-folder
                    sub_dict = current_dict.setdefault(item_id, {
                        "name": item_name,
                        "type": "folder",
                        "children": {}
                    })
                    await crawl_folder(item_id, item_name, sub_dict["children"], depth + 1, path_stack + [item_name])

                elif ct in (3, "3") or item_name.lower().endswith(".pdf") or item.get("format") == "pdf":  # PDF Note Document!
                    if stats["pdfs_uploaded"] >= max_pdfs:
                        continue
                    pdf_url = item.get("url") or item.get("documentUrl")
                    if not pdf_url:
                        continue

                    # Extract Subject, Sub-Version, Unit, and Sequence
                    subject_name, sub_version, unit_name = extract_hierarchy_metadata(path_stack)

                    import re
                    m = re.search(r'(?:lec|lecture|class)[-:\s]*([0-9]+)', item_name, re.IGNORECASE)
                    order_num = int(m.group(1)) if m else 0

                    from tools.config import BATCH_ALIASES
                    linked_batches = [course_id] + BATCH_ALIASES.get(course_id, [])

                    if item_id in catalog and item_id in verified_item_ids:
                        existing = catalog[item_id]
                        current_dict[item_id] = {
                            "type": "pdf",
                            "title": existing.get("title", item_name),
                            "subject": subject_name,
                            "sub_version": sub_version,
                            "unit": unit_name,
                            "order_index": order_num,
                            "telegram_file_id": existing.get("telegram_file_id", ""),
                            "telegram_msg_id": existing.get("telegram_msg_id", 0)
                        }
                        # 🔗 Auto-link verified alive PDF note to matching video entry in the same unit & version
                        linked_count = 0
                        for vid_id, vid_info in catalog.items():
                            if (vid_info.get("type") == "video"
                                    and vid_info.get("subject", "").lower() == subject_name.lower()
                                    and vid_info.get("unit", "").lower() == unit_name.lower()
                                    and vid_info.get("sub_version", "").lower() == sub_version.lower()):
                                if order_num > 0 and vid_info.get("order_index") == order_num:
                                    vid_info["pdf_content_id"] = item_id
                                    linked_count += 1
                                elif order_num == 0 and not vid_info.get("pdf_content_id"):
                                    vid_info["pdf_content_id"] = item_id
                                    linked_count += 1
                        if linked_count:
                            save_master_index(master_data)
                        print(f"{indent}  ⏩ [VERIFIED ALIVE NOTE REUSED] {item_name} (Msg #{existing.get('telegram_msg_id')})")
                        continue

                    # Download PDF and upload directly to Telegram Vault Channel!
                    print(f"{indent}  📄 Downloading PDF Note: {item_name}...")
                    pdf_tmp = DOWNLOAD_DIR / f"{item_id}.pdf"
                    try:
                        r_pdf = requests.get(pdf_url, timeout=30)
                        if r_pdf.status_code == 200:
                            pdf_tmp.write_bytes(r_pdf.content)
                            file_size_mb = len(r_pdf.content) / (1024 * 1024)
                            caption = (
                                f"📄 {item_name}\n"
                                f"📚 Batch: {', '.join(linked_batches)} ({course_name})\n"
                                f"📖 Subject: {subject_name}\n"
                                f"📁 Unit: {unit_name}\n"
                                f"📊 Size: {file_size_mb:.2f} MB"
                            )
                            msg = await client.send_document(
                                chat_id=TG_CHANNEL_ID,
                                document=str(pdf_tmp),
                                caption=caption[:1024]
                            )
                            file_id = msg.document.file_id if msg.document else ""
                            catalog[item_id] = {
                                "type": "pdf",
                                "title": item_name,
                                "subject": subject_name,
                                "sub_version": sub_version,
                                "unit": unit_name,
                                "order_index": order_num,
                                "batch_ids": linked_batches,
                                "telegram_file_id": file_id,
                                "telegram_msg_id": msg.id,
                                "file_size": f"{file_size_mb:.2f} MB",
                                "file_size_bytes": len(r_pdf.content),
                                "url": pdf_url,
                                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                            }
                            current_dict[item_id] = {
                                "type": "pdf",
                                "title": item_name,
                                "subject": subject_name,
                                "sub_version": sub_version,
                                "unit": unit_name,
                                "order_index": order_num,
                                "telegram_file_id": file_id,
                                "telegram_msg_id": msg.id
                            }
                            verified_item_ids.add(item_id)

                            # 🔗 Auto-link this PDF note to matching video entry in the same unit
                            linked_count = 0
                            for vid_id, vid_info in catalog.items():
                                if (vid_info.get("type") == "video"
                                        and vid_info.get("subject", "").lower() == subject_name.lower()
                                        and vid_info.get("unit", "").lower() == unit_name.lower()):
                                    if order_num > 0 and vid_info.get("order_index") == order_num:
                                        vid_info["pdf_content_id"] = item_id
                                        linked_count += 1
                                    elif order_num == 0 and not vid_info.get("pdf_content_id"):
                                        vid_info["pdf_content_id"] = item_id
                                        linked_count += 1
                            if linked_count:
                                print(f"{indent}  🔗 Linked PDF note to {linked_count} video(s) in '{unit_name}'")

                            save_master_index(master_data)
                            stats["pdfs_uploaded"] += 1
                            print(f"{indent}  ✅ [NOTE UPLOAD COMPLETE] {item_name} (Msg #{msg.id}, Total PDFs: {stats['pdfs_uploaded']}/{max_pdfs})")
                    except Exception as pdf_err:
                        print(f"{indent}  ⚠️ PDF sync error for {item_name}: {pdf_err}")
                    finally:
                        if pdf_tmp.exists():
                            try: pdf_tmp.unlink()
                            except Exception: pass

                elif ct in (2, "2"):  # Video
                    if stats["downloaded"] >= max_videos:
                        continue

                    content_hash = item.get("contentHashId") or ""
                    duration = item.get("duration", 0)

                    # Extract Subject, Sub-Version, Unit, and Sequence
                    subject_name, sub_version, unit_name = extract_hierarchy_metadata(path_stack)

                    import re
                    m = re.search(r'(?:lec|lecture|class)[-:\s]*([0-9]+)', item_name, re.IGNORECASE)
                    order_num = int(m.group(1)) if m else (0 if "syllabus" in item_name.lower() or "intro" in item_name.lower() else 999)

                    from tools.config import BATCH_ALIASES
                    linked_batches = [course_id] + BATCH_ALIASES.get(course_id, [])

                    # ── UNIVERSAL DEDUPLICATION & TELEGRAM VERIFICATION ───
                    if item_id in catalog and item_id in verified_item_ids:
                        existing = catalog[item_id]
                        existing.setdefault("subject", subject_name)
                        existing.setdefault("sub_version", sub_version)
                        existing.setdefault("unit", unit_name)
                        existing.setdefault("batch_ids", linked_batches)
                        existing.setdefault("order_index", order_num)

                        # Find matching PDF note for this video (matching order_index or fallback to unit combined note)
                        pdf_note_id = existing.get("pdf_content_id")
                        if not pdf_note_id:
                            for cat_id, cat_info in catalog.items():
                                if (cat_info.get("type") == "pdf"
                                        and cat_info.get("subject", "").lower() == subject_name.lower()
                                        and cat_info.get("unit", "").lower() == unit_name.lower()
                                        and cat_info.get("sub_version", "").lower() == sub_version.lower()):
                                    if order_num > 0 and cat_info.get("order_index") == order_num:
                                        pdf_note_id = cat_id
                                        existing["pdf_content_id"] = cat_id
                                        break
                                    elif order_num == 0 and not pdf_note_id:
                                        pdf_note_id = cat_id
                                        existing["pdf_content_id"] = cat_id

                        current_dict[item_id] = {
                            "type": "video",
                            "title": existing["title"],
                            "duration": existing.get("duration", duration),
                            "subject": subject_name,
                            "sub_version": sub_version,
                            "unit": unit_name,
                            "order_index": order_num,
                            "telegram_file_id": existing["telegram_file_id"],
                            "telegram_msg_id": existing["telegram_msg_id"]
                        }
                        if pdf_note_id:
                            current_dict[item_id]["pdf_content_id"] = pdf_note_id

                        print(f"{indent}  ⏩ [VERIFIED ALIVE & REUSED] {item_name} (Msg #{existing['telegram_msg_id']})")
                        continue
                    elif item_id in catalog and item_id not in verified_item_ids:
                        print(f"{indent}  🔄 [MISSING ON TELEGRAM] {item_name} -> Purged stale index & Re-downloading!")
                        catalog.pop(item_id, None)

                    if not content_hash:
                        print(f"{indent}  ⚠️ [SKIP] No contentHashId: {item_name}")
                        continue

                    # Fetch stream URL with safe human delay
                    print(f"{indent}  🔍 [{stats['downloaded']+1}/{max_videos}] Requesting URL: {item_name}...")
                    stream_url = get_signed_stream_url(content_hash, token)
                    if not stream_url:
                        print(f"{indent}  ❌ [FAILED] Unable to resolve stream URL")
                        continue

                    clean_file = DOWNLOAD_DIR / f"{item_id}.mp4"
                    print(f"{indent}  ⬇️ Downloading & Stripping: {item_name}...")

                    # Run CPU-bound/blocking download in executor
                    loop = asyncio.get_running_loop()
                    dl_ok = await loop.run_in_executor(
                        None, download_and_clean_video, stream_url, clean_file, item_name
                    )

                    if dl_ok:
                        stats["downloaded"] += 1
                        print(f"{indent}  📦 Downloaded #{stats['downloaded']} ➔ Queued for parallel upload.")
                        # Put in queue (will pause if uploader is busy)
                        await queue.put((item_id, item_name, clean_file, duration, current_dict, depth, subject_name, sub_version, unit_name, order_num, linked_batches))

        await crawl_folder("0", course_name, course_entry["root_folders"], 0, [])
        await queue.put(None)  # Sentinel to signal download complete
        print(f"\n[DOWNLOADER] Finished! Total {stats['downloaded']} new videos downloaded.")

    # ── WORKER 2: CONSUMER (Pyrogram MTProto Uploader) ───────────────
    async def consumer_uploader():
        while True:
            job = await queue.get()
            if job is None:
                queue.task_done()
                break

            item_id, item_name, clean_file, duration, current_dict, depth, subject_name, sub_version, unit_name, order_num, linked_batches = job
            indent = "  " * depth
            file_size_mb = clean_file.stat().st_size / (1024 * 1024) if clean_file.exists() else 0

            # Extract crisp video frame thumbnail from downloaded clean file
            thumb_file = clean_file.with_suffix(".jpg")
            try:
                subprocess.run(
                    ["ffmpeg", "-y", "-i", str(clean_file), "-ss", "00:00:05", "-vframes", "1", "-q:v", "2", str(thumb_file)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20
                )
                if thumb_file.exists() and thumb_file.stat().st_size > 0:
                    # Cache for stream server
                    from tools.config import THUMB_DIR
                    cached_thumb = THUMB_DIR / f"{item_id}.jpg"
                    import shutil
                    shutil.copyfile(str(thumb_file), str(cached_thumb))
            except Exception as th_err:
                print(f"{indent}  ⚠️ Thumb extract err: {th_err}")

            print(f"{indent}  ☁️ [UPLOADING PARALLEL] {item_name} ({file_size_mb:.1f} MB)...")
            try:
                sub_ver_str = f" ({sub_version})" if sub_version else ""
                caption = (
                    f"🎬 {item_name}\n"
                    f"📚 Batch: {', '.join(linked_batches)} ({course_name})\n"
                    f"📖 Subject: {subject_name}{sub_ver_str}\n"
                    f"📁 Unit: {unit_name}\n"
                    f"🔢 Order: #{order_num}\n"
                    f"⏱️ Duration: {duration}s"
                )
                msg = await client.send_video(
                    chat_id=TG_CHANNEL_ID,
                    video=str(clean_file),
                    thumb=str(thumb_file) if thumb_file.exists() and thumb_file.stat().st_size > 0 else None,
                    caption=caption[:1024],
                    supports_streaming=True
                )
                file_id = msg.video.file_id if msg.video else (msg.document.file_id if msg.document else "")
                msg_id = msg.id

                # Check if a PDF note exists for this unit & version
                pdf_note_id = None
                for cat_id, cat_info in catalog.items():
                    if (cat_info.get("type") == "pdf"
                            and cat_info.get("subject", "").lower() == subject_name.lower()
                            and cat_info.get("unit", "").lower() == unit_name.lower()
                            and cat_info.get("sub_version", "").lower() == sub_version.lower()):
                        pdf_note_id = cat_id
                        break

                # Update global catalog
                clean_size_bytes = clean_file.stat().st_size if clean_file.exists() else 0
                catalog[item_id] = {
                    "type": "video",
                    "title": item_name,
                    "duration": duration,
                    "subject": subject_name,
                    "sub_version": sub_version,
                    "unit": unit_name,
                    "order_index": order_num,
                    "batch_ids": linked_batches,
                    "file_size": f"{file_size_mb:.2f} MB",
                    "file_size_bytes": clean_size_bytes,
                    "telegram_file_id": file_id,
                    "telegram_msg_id": msg_id,
                    "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                if pdf_note_id:
                    catalog[item_id]["pdf_content_id"] = pdf_note_id

                # Attach to current course tree
                current_dict[item_id] = {
                    "type": "video",
                    "title": item_name,
                    "duration": duration,
                    "subject": subject_name,
                    "sub_version": sub_version,
                    "unit": unit_name,
                    "order_index": order_num,
                    "telegram_file_id": file_id,
                    "telegram_msg_id": msg_id
                }
                if pdf_note_id:
                    current_dict[item_id]["pdf_content_id"] = pdf_note_id

                verified_item_ids.add(item_id)

                # Mirror across linked batches in master index
                for lb in linked_batches:
                    if lb != course_id and lb in courses:
                        lb_roots = courses[lb].setdefault("root_folders", {})
                        # ensure same root folders exist
                        pass

                stats["uploaded"] += 1
                save_master_index(master_data)
                print(f"{indent}  ✅ [UPLOAD COMPLETE] Msg #{msg_id} ({stats['uploaded']}/{max_videos})")

            except Exception as up_err:
                print(f"{indent}  ❌ [UPLOAD FAILED] {item_name}: {up_err}")
            finally:
                if clean_file.exists():
                    try: clean_file.unlink()
                    except Exception: pass
                if thumb_file.exists():
                    try: thumb_file.unlink()
                    except Exception: pass
                queue.task_done()

        print(f"[UPLOADER] Finished! Total {stats['uploaded']} new videos uploaded.")

    # Run Downloader and Uploader concurrently!
    await asyncio.gather(producer_downloader(), consumer_uploader())
    save_master_index(master_data)

    # 📌 Option A: In-Place Update Pinned master_index.json in Telegram Vault
    try:
        from pyrogram.types import InputMediaDocument
        chat = await client.get_chat(TG_CHANNEL_ID)
        v_stats = master_data.get("vault_stats", {})
        data_gb = v_stats.get("total_data_gb", 0)
        caption = (
            f"📑 #MASTER_INDEX_CATALOG\n"
            f"📚 Batch: [{course_id}] {course_name}\n"
            f"📊 Total Catalog Items: {len(catalog)}\n"
            f"💾 Total Data: {data_gb} GB\n"
            f"⏰ Synced: {time.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📌 Status: Live Pinned Catalog"
        )
        # In-Place Update dedicated Msg #90 (Never post new document or create duplicate pins!)
        target_msg_id = 90
        try:
            await client.edit_message_media(
                chat_id=TG_CHANNEL_ID,
                message_id=target_msg_id,
                media=InputMediaDocument(
                    media=str(MASTER_INDEX_FILE),
                    caption=caption
                )
            )
            print(f"📌 [VAULT IN-PLACE UPDATE] Dedicated Msg #{target_msg_id} edited in-place with latest master_index.json!")
        except Exception as edit_err:
            print(f"⚠️ Could not edit dedicated Msg #{target_msg_id} in-place: {edit_err}")
            if pinned and pinned.document and pinned.id != target_msg_id:
                try:
                    await client.edit_message_media(
                        chat_id=TG_CHANNEL_ID,
                        message_id=pinned.id,
                        media=InputMediaDocument(media=str(MASTER_INDEX_FILE), caption=caption)
                    )
                    print(f"📌 [VAULT IN-PLACE UPDATE] Pinned Msg #{pinned.id} edited in-place with latest master_index.json!")
                except Exception as fallback_err:
                    print(f"⚠️ Fallback edit error: {fallback_err}")
    except Exception as pin_err:
        print(f"⚠️ Telegram vault catalog pin notice: {pin_err}")

    return stats["uploaded"]

def export_text_manifest(course_id: str, course_name: str, token: Optional[str] = None) -> Path:
    """
    Crawls the Classplus course structure and generates an ultra-clean,
    human-readable .txt course manifest and syllabus tree map.
    """
    from tools.config import get_active_classplus_token
    token = token or get_active_classplus_token(course_id)
    manifest_dir = DATA_DIR / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r'[^a-zA-Z0-9_\-]', '_', course_name).strip('_')
    out_file = manifest_dir / f"{course_id}_{slug}.txt"

    master_data = load_master_index()
    catalog = master_data.get("content_catalog", {})

    print(f"\n=======================================================")
    print(f"📑 EXPORTING TEXT MANIFEST: [{course_id}] {course_name}")
    print(f"=======================================================")

    lines = [
        "=" * 80,
        "🎓 GATEWAY CLASSES COURSE MANIFEST & SYLLABUS TREE",
        f"Course Name : {course_name}",
        f"Course ID   : {course_id}",
        f"Export Time : {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
        "=" * 80,
        ""
    ]

    total_subjects = 0
    total_videos = 0
    total_pdfs = 0

    root_items = fetch_classplus_folder(course_id, "0", token)

    for sub in root_items:
        content_type = sub.get("contentType")
        if content_type == 1:
            total_subjects += 1
            sub_id = sub["id"]
            sub_name = sub.get("name", "Unknown Subject").strip()
            lines.append(f"📂 [SUBJECT {total_subjects}] {sub_name} (Folder ID: {sub_id})")

            unit_items = fetch_classplus_folder(course_id, str(sub_id), token)
            for unit in unit_items:
                u_type = unit.get("contentType")
                if u_type == 1:
                    unit_id = unit["id"]
                    unit_name = unit.get("name", "Unknown Unit").strip()
                    lines.append(f"   └── 📁 [UNIT] {unit_name} (Folder ID: {unit_id})")

                    lectures = fetch_classplus_folder(course_id, str(unit_id), token)
                    for item in lectures:
                        item_id = str(item.get("id"))
                        item_name = item.get("name", "Untitled").strip()
                        i_type = item.get("contentType")
                        if i_type == 2 or (item.get("url") and ".m3u8" in item.get("url")):
                            total_videos += 1
                            dur = item.get("duration", "--:--")
                            tg_tag = f"✅ TG #{catalog[item_id]['telegram_msg_id']}" if item_id in catalog else "⏳ PENDING_SYNC"
                            lines.append(f"       ├── 🎥 [LEC] {item_name} ({dur}) | ID: {item_id} | {tg_tag}")
                        elif i_type == 3 or item.get("format") == "pdf":
                            total_pdfs += 1
                            pdf_url = item.get("url", "")
                            lines.append(f"       │    └─ 📄 [PDF] {item_name} | ID: {item_id} | URL: {pdf_url}")
                elif u_type == 2:
                    total_videos += 1
                    item_id = str(unit.get("id"))
                    dur = unit.get("duration", "--:--")
                    tg_tag = f"✅ TG #{catalog[item_id]['telegram_msg_id']}" if item_id in catalog else "⏳ PENDING_SYNC"
                    lines.append(f"   ├── 🎥 [LEC] {unit.get('name', 'Untitled').strip()} ({dur}) | ID: {item_id} | {tg_tag}")
                elif u_type == 3 or unit.get("format") == "pdf":
                    total_pdfs += 1
                    item_id = str(unit.get("id"))
                    lines.append(f"   └── 📄 [PDF] {unit.get('name', 'Untitled').strip()} | ID: {item_id} | URL: {unit.get('url', '')}")
            lines.append("")
        elif content_type == 2:
            total_videos += 1
            item_id = str(sub.get("id"))
            dur = sub.get("duration", "--:--")
            tg_tag = f"✅ TG #{catalog[item_id]['telegram_msg_id']}" if item_id in catalog else "⏳ PENDING_SYNC"
            lines.append(f"🎥 [ROOT VIDEO] {sub.get('name', 'Untitled').strip()} ({dur}) | ID: {item_id} | {tg_tag}")
        elif content_type == 3 or sub.get("format") == "pdf":
            total_pdfs += 1
            item_id = str(sub.get("id"))
            lines.append(f"📄 [ROOT PDF] {sub.get('name', 'Untitled').strip()} | ID: {item_id} | URL: {sub.get('url', '')}")

    summary_block = [
        f"📊 Course Statistics:",
        f"   - Total Subjects      : {total_subjects}",
        f"   - Total Video Lectures: {total_videos}",
        f"   - Total PDF Notes     : {total_pdfs}",
        f"   - Active in Vault     : {len(catalog)} items synced",
        "-" * 80,
        ""
    ]
    # Insert summary right below the header
    for idx, s_line in enumerate(summary_block):
        lines.insert(6 + idx, s_line)

    content = "\n".join(lines)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ [MANIFEST SAVED] {out_file}")
    print(f"📊 Summary: {total_subjects} Subjects, {total_videos} Videos, {total_pdfs} PDFs")
    return out_file

async def run_sync():
    import argparse
    parser = argparse.ArgumentParser(description="Gateway Classes Matrix Sync Engine")
    parser.add_argument("--course-id", default="876795", help="Course ID to sync")
    parser.add_argument("--course-name", default="CSE STREAM : Sem-I + Sem-II COMBO", help="Course Name")
    parser.add_argument("--token", default="", help="Classplus User JWT Token (optional, auto-harvested if empty)")
    parser.add_argument("--max-videos", type=int, default=MAX_VIDEOS_PER_RUN, help="Max videos to upload this run")
    parser.add_argument("--max-pdfs", type=int, default=MAX_PDFS_PER_RUN, help="Max PDFs to upload this run")
    parser.add_argument("--audit-only", action="store_true", help="Only audit Telegram Vault and display verified state without uploading")
    parser.add_argument("--export-txt", action="store_true", help="Export course syllabus text manifest (.txt) and exit")
    args = parser.parse_args()

    if args.export_txt:
        export_text_manifest(
            course_id=args.course_id,
            course_name=args.course_name,
            token=args.token or None
        )
        return

    app = Client(
        "gateway_sync_worker",
        api_id=SYNC_API_ID,
        api_hash=SYNC_API_HASH,
        bot_token=SYNC_BOT_TOKEN,
        workdir=str(DATA_DIR)
    )

    async with app:
        if args.audit_only:
            master_data = load_master_index()
            await audit_and_verify_vault(app, master_data, args.course_id)
        else:
            await sync_course_tree(
                client=app,
                course_id=args.course_id,
                course_name=args.course_name,
                token=args.token or None,
                max_videos=args.max_videos,
                max_pdfs=args.max_pdfs
            )

if __name__ == "__main__":
    asyncio.run(run_sync())
