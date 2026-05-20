import os
import asyncio
import shutil
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from hydrogram import Client, filters
from hydrogram.types import Message

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "http://localhost:8000")

# 1. 快取設定：建立資料夾並限制最大硬碟佔用量
CACHE_DIR = "/app/video_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
MAX_CACHE_SIZE_GB = 5.0  # 💡 設定快取上限為 5GB，防止 Render 容器硬碟爆掉

@asynccontextmanager
async def lifespan(app: FastAPI):
    bot_task = asyncio.create_task(bot.start())
    print(f"🚀 安全快取串流伺服器已啟動！最大快取上限: {MAX_CACHE_SIZE_GB}GB")
    yield
    await bot.stop()
    bot_task.cancel()

app = FastAPI(lifespan=lifespan)
bot = Client("stream_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

# 2. 自動清理舊快取函數 (LRU 演算法)
def auto_cleanup_cache():
    try:
        # 計算目前快取資料夾的總大小
        total_size = sum(os.path.getsize(os.path.join(CACHE_DIR, f)) for f in os.listdir(CACHE_DIR) if os.path.isfile(os.path.join(CACHE_DIR, f)))
        max_size_bytes = MAX_CACHE_SIZE_GB * 1024 * 1024 * 1024
        
        if total_size <= max_size_bytes:
            return
            
        print(f"⚠️ 快取容量 ({total_size / (1024**3):.2f}GB) 超過上限 ({MAX_CACHE_SIZE_GB}GB)，開始自動清理舊檔案...")
        
        # 將檔案依照「最後讀取時間 (atime)」排序，最久沒人看排最前面
        files = [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if os.path.isfile(os.path.join(CACHE_DIR, f))]
        files.sort(key=lambda x: os.path.getatime(x))
        
        # 依序刪除最舊的檔案，直到容量降回安全範圍內
        for file_path in files:
            if total_size <= max_size_bytes:
                break
            file_size = os.path.getsize(file_path)
            os.remove(file_path)
            total_size -= file_size
            print(f"♻️ 已自動刪除最久未使用的快取檔案: {os.path.basename(file_path)}")
            
        print("✅ 快取清理完畢，硬碟空間已恢復安全狀態。")
    except Exception as e:
        print(f"❌ 快取清理過程中發生錯誤: {e}")

# 3. 背景下載常駐任務：下載完成後自動觸發清理
async def background_downloader(file_id: str, local_path: str):
    try:
        print(f"📥 開始背景下載檔案至容器: {local_path}")
        await bot.download_media(file_id, file_name=local_path)
        print(f"✅ 檔案已完整下載並儲存至容器硬碟: {local_path}")
        
        # 💡 下載成功後，立刻檢查並清理舊快取，確保容量不超標
        auto_cleanup_cache()
    except Exception as e:
        print(f"❌ 背景下載失敗: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)

# 4. 混合式串流生成器
async def hybrid_chunk_generator(file_properties, file_id: str, start_byte: int, end_byte: int, local_path: str):
    # 情況 A：如果硬碟裡有完整快取，直接秒讀硬碟！
    if os.path.exists(local_path) and os.path.getsize(local_path) == file_properties.file_size:
        # 💡 每次讀取時，更新檔案的存取時間，確保它不會被當成舊檔案刪掉
        try: os.utime(local_path, None) 
        except: pass
        
        print("⚡ [Cache Hit] 偵測到本機完整檔案，直接從容器硬碟讀取！")
        with open(local_path, "rb") as f:
            f.seek(start_byte)
            remaining = end_byte - start_byte + 1
            chunk_size = 1024 * 1024
            while remaining > 0:
                to_read = min(chunk_size, remaining)
                data = f.read(to_read)
                if not data:
                    break
                yield data
                remaining -= len(data)
        return

    # 情況 B：硬碟沒快取，走 MTProto 線上串流，並在背景默默把檔案存下來
    print("🌐 [Cache Miss] 容器內無快取，從 Telegram MTProto 下載，並啟動背景儲存...")
    asyncio.create_task(background_downloader(file_id, local_path))
    
    tg_chunk_size = 1024 * 1024
    start_chunk = start_byte // tg_chunk_size
    async for chunk in bot.stream_media(file_properties, offset=start_chunk):
        if not chunk:
            break
        yield bytes(chunk)

@bot.on_message(filters.video | filters.document)
async def handle_media(client: Client, message: Message):
    media = message.video or message.document
    if not media: return
    unique_file_id = media.file_id
    base_domain = DOMAIN.rstrip('/')
    
    await message.reply_text(
        f"🎬 **智慧快取串流服務已就緒！**\n\n"
        f"🌐 **網頁瀏覽器直接看：**\n`{base_domain}/watch/{unique_file_id}`\n\n"
        f"📺 **VLC / PotPlayer 直鏈：**\n`{base_domain}/stream/{unique_file_id}`\n\n"
        f"💡 備註：系統會自動下載並保留熱門影片，硬碟快滿時會自動清理舊檔案！"
    )

@app.get("/stream/{file_id}")
async def stream_endpoint(file_id: str, range: str = Header(None)):
    try:
        file_properties = await bot.get_file(file_id)
        if not file_properties:
            raise HTTPException(status_code=404, detail="檔案不存在")
            
        file_size = file_properties.file_size
        start, end = 0, file_size - 1

        if range and range.startswith("bytes="):
            try:
                range_str = range.replace("bytes=", "")
                parts = range_str.split("-")
                if parts: start = int(parts[0])
                if len(parts) > 1 and parts[1]: end = int(parts[1])
            except Exception:
                pass

        local_path = os.path.join(CACHE_DIR, f"{file_id}.mp4")

        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": "video/mp4",
            "Cache-Control": "no-cache",
        }
        
        return StreamingResponse(
            hybrid_chunk_generator(file_properties, file_id, start, end, local_path), 
            status_code=206, 
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/watch/{file_id}", response_class=HTMLResponse)
async def watch_video_page(file_id: str):
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>智慧快取串流播放器</title>
        <style>
            body {{ margin: 0; background: #000; color: #fff; display: flex; justify-content: center; align-items: center; height: 100vh; }}
            video {{ width: 100%; max-width: 850px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.8); }}
        </style>
    </head>
    <body>
        <video controls autoplay preload="auto" playsinline><source src="/stream/{file_id}" type="video/mp4"></video>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

@app.get("/")
async def index():
    # 可以在首頁直接看到目前快取了哪些檔案
    return {
        "status": "running", 
        "max_limit_gb": MAX_CACHE_SIZE_GB,
        "cached_files": os.listdir(CACHE_DIR)
    }
