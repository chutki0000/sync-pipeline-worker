#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration manager for Gateway Classes Matrix Sync.
Loads secrets from environment variables or local .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# 1. MTProto Video Streaming Server & Telegram Admin Bot (@leechm35_bot)
STREAM_API_ID = int(os.getenv("STREAM_API_ID", os.getenv("TG_API_ID", "0")))
STREAM_API_HASH = os.getenv("STREAM_API_HASH", os.getenv("TG_API_HASH", ""))
STREAM_BOT_TOKEN = os.getenv("STREAM_BOT_TOKEN", os.getenv("TG_BOT_TOKEN", ""))

# 2. Matrix Sync Engine & Video Downloader/Uploader Bot (Gatey - @Gatewayclassesofficial_bot)
SYNC_API_ID = int(os.getenv("SYNC_API_ID", "0"))
SYNC_API_HASH = os.getenv("SYNC_API_HASH", "")
SYNC_BOT_TOKEN = os.getenv("SYNC_BOT_TOKEN", "")

# Shared Telegram Vault Channel & Admin Config
TG_CHANNEL_ID = int(os.getenv("TG_CHANNEL_ID", "0"))
TG_API_ID = STREAM_API_ID
TG_API_HASH = STREAM_API_HASH
TG_BOT_TOKEN = STREAM_BOT_TOKEN

# Bot Owner & Admin Configuration
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]


# Classplus Sync Defaults
CLASSPLUS_TOKEN = os.getenv("CLASSPLUS_TOKEN", "")
CLASSPLUS_ORG_CODE = os.getenv("CLASSPLUS_ORG_CODE", "mvxiz")
CLASSPLUS_ORG_ID = int(os.getenv("CLASSPLUS_ORG_ID", "436362"))

# Rate Limiting & Safety Controls (Zero Ban Architecture)
SAFETY_REQUEST_DELAY = float(os.getenv("SAFETY_REQUEST_DELAY", "2.5")) # 2.5 sec pause between URL fetches
MAX_VIDEOS_PER_RUN = int(os.getenv("MAX_VIDEOS_PER_RUN", "20"))       # Max 20 videos per run
MAX_PDFS_PER_RUN = int(os.getenv("MAX_PDFS_PER_RUN", "20"))           # Max 20 PDFs per run

# File paths
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MASTER_INDEX_FILE = DATA_DIR / "master_index.json"
USERS_ACCESS_FILE = DATA_DIR / "users_access.json"
def get_active_classplus_token(course_id: str = None) -> str:
    """
    Returns active Classplus token from environment or CLI argument.
    """
    return os.getenv("CLASSPLUS_TOKEN", "").strip()
