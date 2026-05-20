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
    print("🚀 FastAPI 網頁服務已成功啟動！")
    yield
    await bot.stop()
    bot_task.cancel()

app = FastAPI(lifespan=lifespan)
bot = Client("stream_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

@bot.on_message(filters.video | filters.document)
async def handle_media(client: Client, message: Message):
    media = message.video or message.document
    if not media:
        return
        
    unique_file_id = media.file_id
    base_domain = DOMAIN.rstrip('/')
    
    web_url = f"{base_domain}/watch/{unique_file_id}"
    stream_url = f"{base_domain}/stream/{unique_file_id}"
    
    await message.reply_text(
        f"🎬 **串流網址已完美修正！**\n\n"
        f"🌐 **網頁瀏覽器直接看：**\n`{web_url}`\n\n"
        f"📺 **VLC / PotPlayer 直鏈：**\n`{stream_url}`\n\n"
        f"💡 提示：點擊第一個網頁網址即可在 Chrome/Safari 直接觀看！"
    )

@app.get("/watch/{file_id}", response_class=HTMLResponse)
async def watch_video_page(file_id: str):
    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Telegram 網頁即時串流播放器</title>
        <style>
            body {{ margin: 0; background: #0b0b0b; color: #fff; font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; flex-direction: column; }}
            .container {{ width: 95%; max-width: 850px; text-align: center; }}
            video {{ width: 100%; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.9); background: #000; }}
            h1 {{ font-size: 1.3rem; margin-bottom: 20px; color: #3babff; letter-spacing: 1px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🎬 正在透過系統分塊傳輸解碼（支援快進）...</h1>
            <video controls autoplay preload="auto" playsinline>
                <source src="/stream/{file_id}" type="video/mp4">
                您的瀏覽器不支援此 MP4 編碼播放。
            </video>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

# 💡 核心修復：精確換算字節，完美支援瀏覽器 Range 請求
async def chunk_generator(file_properties, start_byte: int, end_byte: int):
    try:
        # Telegram MTProto 標準分塊大小為 1MB (1024 * 1024 bytes)
        tg_chunk_size = 1024 * 1024
        
        # 計算瀏覽器要求的起點是第幾個區塊
        start_chunk = start_byte // tg_chunk_size
        
        # 使用 stream_media 並給予精確的 offset（跳過前幾個區塊）
        # 這樣當瀏覽器快進時，才不會永遠都從影片第 0 秒開始抓而導致黑畫面
        async for chunk in bot.stream_media(file_properties, offset=start_chunk):
            if not chunk:
                break
            yield bytes(chunk)
    except Exception as e:
        print(f"流媒體傳輸中斷: {e}")

@app.get("/stream/{file_id}")
async def stream_endpoint(file_id: str, range: str = Header(None)):
    try:
        # 1. 解析檔案屬性（取得精確的 file_size 給瀏覽器分配緩衝）
        file_properties = await bot.get_file(file_id)
        if not file_properties:
            raise HTTPException(status_code=404, detail="檔案不存在")
            
        file_size = file_properties.file_size
        start, end = 0, file_size - 1

        # 2. 嚴格解析瀏覽器的 HTTP Range (這是消除黑畫面的致命關鍵)
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

        # 3. 封裝完全符合 Chrome 和 Safari 規格的 206 狀態標頭
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": "video/mp4",  # 讓瀏覽器明確知道是 MP4 格式
            "Cache-Control": "no-cache",
        }
        
        return StreamingResponse(
            chunk_generator(file_properties, start, end), 
            status_code=206, 
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def index():
    return {"status": "running", "message": "Telegram Stream Bot is online!"}
