#!/usr/bin/env python3
"""
Download all required input files for load test scripts.
Images: loremflickr (portraits + groups) + picsum (grayscale)
Speech: gTTS generated (single speaker, clean, no copyright)
Music:  archive.org CC music trimmed to 14s via ffmpeg
Video:  archive.org public domain videos trimmed to 15s via ffmpeg
"""
import os
import sys
import time
import json
import subprocess
import requests
from pathlib import Path
from gtts import gTTS

FFMPEG = "C:/Users/Lenovo/ffmpeg_extracted/ffmpeg-master-latest-win64-gpl/bin/ffmpeg.exe"
BASE   = Path(__file__).parent / "INPUTS"
S      = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0"})

# ─────────────────────────────── helpers ──────────────────────────────────────

def dl(url, path, min_size=8000, timeout=90):
    path = Path(path)
    if path.exists() and path.stat().st_size > min_size:
        print(f"  SKIP {path.name} (exists)")
        return True
    try:
        r = S.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and len(r.content) >= min_size:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(r.content)
            print(f"  OK   {path.name} ({len(r.content)//1024}KB)")
            return True
        print(f"  FAIL {path.name} [{r.status_code} / {len(r.content)}B] {url}")
        return False
    except Exception as e:
        print(f"  ERR  {path.name}: {e}")
        return False

def ffmpeg_trim(src, dst, duration=14):
    dst = Path(dst)
    if dst.exists() and dst.stat().st_size > 5000:
        print(f"  SKIP {dst.name} (exists)")
        return True
    dst.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [FFMPEG, "-y", "-i", str(src), "-t", str(duration),
         "-c:a", "libmp3lame", "-q:a", "4", str(dst)],
        capture_output=True
    )
    if dst.exists() and dst.stat().st_size > 3000:
        print(f"  OK   {dst.name} ({dst.stat().st_size//1024}KB)")
        Path(src).unlink(missing_ok=True)
        return True
    print(f"  FAIL trim {dst.name}")
    return False

def ffmpeg_trim_video(src, dst, start=0, duration=15):
    dst = Path(dst)
    if dst.exists() and dst.stat().st_size > 50000:
        print(f"  SKIP {dst.name} (exists)")
        return True
    dst.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [FFMPEG, "-y", "-ss", str(start), "-i", str(src),
         "-t", str(duration), "-c:v", "libx264", "-c:a", "aac",
         "-vf", "scale=640:360", str(dst)],
        capture_output=True
    )
    if dst.exists() and dst.stat().st_size > 50000:
        print(f"  OK   {dst.name} ({dst.stat().st_size//1024}KB)")
        Path(src).unlink(missing_ok=True)
        return True
    print(f"  FAIL trim_video {dst.name}")
    return False

def archive_get_mp3(identifier):
    """Return first MP3 download URL from an archive.org item."""
    try:
        r = S.get(f"https://archive.org/metadata/{identifier}/files", timeout=20)
        files = r.json().get("result", [])
        for f in files:
            if f.get("name", "").endswith(".mp3"):
                return f"https://archive.org/download/{identifier}/{f['name']}"
    except Exception:
        pass
    return None

def archive_get_mp4(identifier):
    """Return first MP4 download URL from an archive.org item."""
    try:
        r = S.get(f"https://archive.org/metadata/{identifier}/files", timeout=20)
        files = r.json().get("result", [])
        for f in files:
            n = f.get("name", "")
            if n.endswith(".mp4") and int(f.get("size", 0)) < 200_000_000:
                return f"https://archive.org/download/{identifier}/{n}"
    except Exception:
        pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
# 1. IMAGES — loremflickr portraits & group photos, picsum grayscale
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("IMAGES")
print("="*60)

# BGC — 14 more portrait images  (input7..input20)
print("\n[BGC] Portraits (input7–input20)...")
for i in range(7, 21):
    dl(f"https://loremflickr.com/800/1000/portrait?lock={i+100}",
       BASE / "Background Change/input_images" / f"input{i}.jpg")
    time.sleep(0.3)

# BWC — 7 more color + 7 more B&W  (Color4–Color10, Black4–Black10)
print("\n[BWC] Color portraits (Color4–Color10)...")
for i in range(4, 11):
    dl(f"https://loremflickr.com/800/1000/portrait?lock={i+200}",
       BASE / "BW Colorise/Images" / f"Color{i}.jpg")
    time.sleep(0.3)

print("[BWC] B&W portraits (Black4–Black10)...")
for i in range(4, 11):
    dl(f"https://picsum.photos/seed/{i+200}/800/1000?grayscale",
       BASE / "BW Colorise/Images" / f"Black{i}.jpg")
    time.sleep(0.3)

# IFS TemplateImages — 17 more group photos (Input4..Input20)
print("\n[IFS Template] Group photos (Input4–Input20)...")
for i in range(4, 21):
    dl(f"https://loremflickr.com/800/600/group?lock={i+300}",
       BASE / "Face Swap/TemplateImages" / f"Input{i}.jpg")
    time.sleep(0.3)

# IFS TargetImages — 18 more portraits (Input1–3, Input6–20)
print("\n[IFS Target] Portraits (Input1–3, Input6–20)...")
targets = list(range(1, 4)) + list(range(6, 21))
for idx, n in enumerate(targets):
    dl(f"https://loremflickr.com/800/800/portrait?lock={idx+400}",
       BASE / "Face Swap/TargetImages" / f"Input{n}.jpg")
    time.sleep(0.3)

# T2I reference_images — 19 more group photos (Input1, Input3–20)
print("\n[T2I Ref] Group photos (Input1, Input3–20)...")
refs = [1] + list(range(3, 21))
for idx, n in enumerate(refs):
    dl(f"https://loremflickr.com/800/600/people?lock={idx+500}",
       BASE / "Image Generation/reference_images" / f"Input{n}.jpg")
    time.sleep(0.3)

# ══════════════════════════════════════════════════════════════════════════════
# 2. VOICE CLONE AUDIO — gTTS (clear single speaker, no copyright)
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("VOICE CLONE AUDIO (gTTS)")
print("="*60)

VC_TEXTS = [
    "The morning sun cast long shadows across the empty park as she walked alone thinking quietly.",
    "He picked up the old book and opened it slowly reading the first page with great care.",
    "The conference room was silent as she began to speak clearly and confidently to her team.",
    "Rain tapped gently on the window while she sat by the fireplace with a warm cup of tea.",
    "He called out across the hall and waited patiently for a response that never came.",
    "The project deadline was approaching fast and everyone in the office was working very hard.",
    "She stood at the edge of the cliff and looked out over the vast and calm ocean below.",
    "The children gathered around the table eagerly waiting for the birthday cake to be served.",
    "He adjusted his glasses and looked closely at the document placed carefully in front of him.",
    "The sound of the piano filled the small room with a gentle and peaceful evening melody.",
    "She typed the last line of code and leaned back in her chair with a long deep sigh.",
    "The train slowed down as it approached the station and passengers began to quietly stand up.",
    "He explained the new process to his colleague slowly and in simple clear understandable terms.",
    "The lights flickered once and then the entire building went completely dark and silent.",
    "She answered the phone on the second ring and spoke in a calm and professional voice.",
    "The old café on the corner was always busy in the morning with the smell of fresh coffee.",
    "He walked briskly down the corridor holding a folder of important documents tightly.",
    "The presentation began smoothly and the audience listened with great interest and careful focus.",
    "She folded the letter carefully and placed it inside a small envelope on her desk.",
]

vc_dir = BASE / "Voice Clone/input_audio"
for i, text in enumerate(VC_TEXTS, start=2):
    out = vc_dir / f"speech{i:02d}.mp3"
    if out.exists() and out.stat().st_size > 3000:
        print(f"  SKIP {out.name}")
        continue
    try:
        tts = gTTS(text=text, lang="en", slow=False)
        tts.save(str(out))
        print(f"  OK   {out.name} ({out.stat().st_size//1024}KB)")
    except Exception as e:
        print(f"  ERR  {out.name}: {e}")
    time.sleep(0.8)

# ══════════════════════════════════════════════════════════════════════════════
# 3. VOICE REMIX MUSIC — archive.org CC music trimmed to 14s
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("VOICE REMIX MUSIC (archive.org, trimmed to 14s)")
print("="*60)

# CC-licensed music identifiers from archive.org (instrumental, no vocals)
MUSIC_IDS = [
    "Kevin_MacLeod_Royalty_Free_Music",
    "free-music-archive-cc-music",
    "iuma-the_sugarfriedsuperos",
    "iuma-clockworked",
    "iuma-void_lucy",
    "iuma-bobbywayne",
    "iuma-ostin_drais",
    "iuma-mindfuck",
    "JohnHarrissonMusic",
    "cd_royalty-free-music",
    "RetroMusicLibrary",
    "OpenGoldberg",
    "netlabel_monotonik",
    "netlabel_kahvi",
    "netlabel_8bitpeoples",
    "FreeClassicalMusicMP3",
    "MusicForMoments",
    "PixabayRoyaltyFreeMusic",
    "instrumental-loop",
    "ambient-music-pack",
]

vr_dir = BASE / "Voice Remix"
existing_music = [f for f in vr_dir.iterdir()
                  if f.suffix in (".mp3", ".wav") and not f.name.startswith(".")]
print(f"  Existing: {len(existing_music)} files")
needed = 20 - len(existing_music)
print(f"  Need:     {needed} more")

count = 0
tmp = Path("C:/Users/Lenovo/tmp_music")
tmp.mkdir(exist_ok=True)

for mid in MUSIC_IDS:
    if count >= needed:
        break
    mp3_url = archive_get_mp3(mid)
    if not mp3_url:
        print(f"  SKIP {mid} (no MP3 found)")
        continue
    fname = mp3_url.rsplit("/", 1)[-1]
    tmp_path = tmp / fname
    print(f"  DL   {mid}...")
    if dl(mp3_url, tmp_path, min_size=50000, timeout=120):
        out = vr_dir / f"music{count+1:02d}.mp3"
        if ffmpeg_trim(tmp_path, out, duration=14):
            count += 1
    time.sleep(0.5)

if count < needed:
    print(f"  NOTE: Got {count}/{needed} music files from archive.org")

# ══════════════════════════════════════════════════════════════════════════════
# 4. VFS TEMPLATE VIDEOS — archive.org public domain, trimmed to 15s
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("VFS TEMPLATE VIDEOS (archive.org, trimmed to 15s)")
print("="*60)

# Search archive.org for public domain videos with people talking
def search_archive_videos(query, rows=30):
    try:
        r = S.get(
            "https://archive.org/advancedsearch.php",
            params={"q": query, "rows": rows, "output": "json"},
            timeout=20
        )
        return [d["identifier"] for d in r.json()["response"]["docs"]]
    except Exception:
        return []

VIDEO_QUERIES = [
    "interview people talking AND mediatype:movies",
    "speech people face AND mediatype:movies",
    "educational film people AND mediatype:movies",
    "talk show conversation AND mediatype:movies",
]

vid_dir = BASE / "Face Swap/TemplateVideos"
existing_vids = [f for f in vid_dir.iterdir()
                 if f.suffix in (".mp4", ".mov") and not f.name.startswith(".")]
print(f"  Existing: {len(existing_vids)} files")
needed_vids = 20 - len(existing_vids)
print(f"  Need:     {needed_vids} more")

tmp_vid = Path("C:/Users/Lenovo/tmp_video")
tmp_vid.mkdir(exist_ok=True)

vid_count = 0
tried_ids = set()

for query in VIDEO_QUERIES:
    if vid_count >= needed_vids:
        break
    identifiers = search_archive_videos(query, rows=40)
    print(f"  Query '{query[:40]}' → {len(identifiers)} results")

    for ident in identifiers:
        if vid_count >= needed_vids:
            break
        if ident in tried_ids:
            continue
        tried_ids.add(ident)

        mp4_url = archive_get_mp4(ident)
        if not mp4_url:
            continue

        tmp_path = tmp_vid / f"{ident}.mp4"
        print(f"  DL   {ident}...")
        if dl(mp4_url, tmp_path, min_size=200_000, timeout=180):
            out = vid_dir / f"template_vid{vid_count+1:02d}.mp4"
            if ffmpeg_trim_video(tmp_path, out, start=5, duration=15):
                vid_count += 1
        time.sleep(0.5)

if vid_count < needed_vids:
    print(f"  NOTE: Got {vid_count}/{needed_vids} videos from archive.org")

# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("DOWNLOAD COMPLETE — Final counts:")
print("="*60)
folders = {
    "BGC images":       BASE / "Background Change/input_images",
    "BWC images":       BASE / "BW Colorise/Images",
    "IFS templates":    BASE / "Face Swap/TemplateImages",
    "IFS targets":      BASE / "Face Swap/TargetImages",
    "VFS videos":       BASE / "Face Swap/TemplateVideos",
    "T2I references":   BASE / "Image Generation/reference_images",
    "VC audio":         BASE / "Voice Clone/input_audio",
    "VR music":         BASE / "Voice Remix",
}
for label, folder in folders.items():
    if folder.exists():
        count = len([f for f in folder.iterdir()
                     if f.is_file() and not f.name.startswith(".") and f.suffix != ".txt"])
        status = "OK" if count >= 20 else f"NEED {20-count} MORE"
        print(f"  {label:20s}: {count:3d} files  [{status}]")
