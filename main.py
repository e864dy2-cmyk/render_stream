import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from hydrogram import Client, filters
from hydrogram.types import Message

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "http://localhost:8000")

@asynccontextmanager
async def lifespan(app: FastAPI):
    bot_task = asyncio.create_task(bot.start())
    print("🚀 FastAPI 網頁+串流伺服器已成功就緒！")
    yield
    await bot.stop()
    bot_task.cancel()

app = FastAPI(lifespan=lifespan)

bot = Client("stream_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

# 1. 接收影片事件：同時給予使用者「網頁觀看」與「播放器直鏈」兩種網址
@bot.on_message(filters.video | filters.document)
async def handle_media(client: Client, message: Message):
    media = message.video or message.document
    if not media:
        return
        
    unique_file_id = media.file_id
    base_domain = DOMAIN.rstrip('/')
    
    # 網頁播放網址
    web_url = f"{base_domain}/watch/{unique_file_id}"
    # 播放器直鏈網址
    stream_url = f"{base_domain}/stream/{unique_file_id}"
    
    await message.reply_text(
        f"🎬 **串流服務已成功生成！**\n\n"
        f"🌐 **網頁瀏覽器直接看：**\n`{web_url}`\n\n"
        f"📺 **VLC / PotPlayer 專用直鏈：**\n`{stream_url}`\n\n"
        f"💡 提示：點擊網頁連結即可在瀏覽器內直接免下載播放！"
    )

# 2. 【核心新增】網頁播放器介面：當使用者用瀏覽器打開時，渲染出一個精美的播放器
@app.get("/watch/{file_id}", response_class=HTMLResponse)
async def watch_video_page(file_id: str):
    # 建立一個極簡的 HTML5 原生播放器網頁
    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Telegram 網頁即時串流播放器</title>
        <style>
            body {{ margin: 0; background: #0e0e0e; color: #fff; font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; flex-direction: column; }}
            .container {{ width: 90%; max-width: 800px; text-align: center; }}
            video {{ width: 100%; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.8); background: #000; }}
            h1 {{ font-size: 1.2rem; margin-bottom: 15px; color: #3babff; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🎬 正在透過 MTProto 免下載即時串流播放...</h1>
            <!-- src 直接對接我們下方寫好的串流核心，瀏覽器會自動用 HTTP Range 分塊索取影片 -->
            <video controls autoplay preload="metadata">
                <source src="/stream/{file_id}" type="video/mp4">
                您的瀏覽器不支援 HTML5 影片播放。
            </video>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

# 3. 串流分塊生成器邏輯
async def chunk_generator(file_id: str, start: int, end: int, chunk_size: int):
    offset = start
    while offset <= end:
        current_size = min(chunk_size, end - offset + 1)
        chunk = await bot.download_media(file_id, in_memory=True, offset=offset, limit=current_size)
        if not chunk:
            break
        yield bytes(chunk)
        offset += len(chunk)

# 4. 串流數據核心介面（支援 Chrome / Safari / Firefox 的分塊 Range 快進要求）
@app.get("/stream/{file_id}")
async def stream_endpoint(file_id: str, range: str = Header(None)):
    try:
        file_properties = await bot.get_file(file_id)
        if not file_properties:
            raise HTTPException(status_code=404, detail="檔案不存在")
            
        file_size = file_properties.file_size
        start, end = 0, file_size - 1

        # 完美支援瀏覽器拉動進度條 (Seek)
        if range and range.startswith("bytes="):
            try:
                range_str = range.replace("bytes=", "")
                parts = range_str.split("-")
                if parts[0]:
                    start = int(parts[0])
                if len(parts) > 1 and parts[1]:
                    end = int(parts[1])
            except Exception:
                pass

        CHUNK_SIZE = 1024 * 1024  # 每次讀取 1MB
        
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": "video/mp4",
        }
        return StreamingResponse(chunk_generator(file_id, start, end, CHUNK_SIZE), status_code=206, headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def index():
    return {"status": "running", "message": "Telegram Stream Bot is online!"}
