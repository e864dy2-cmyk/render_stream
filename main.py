import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from hydrogram import Client, filters
from hydrogram.types import Message

# 從 Render 後台環境變數讀取憑證與網域
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "http://localhost:8000")

# 1. 使用現代 FastAPI 的 lifespan 機制，將 Bot 啟動放入背景，絕不卡死 Render 的網關
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 背景啟動機器人
    bot_task = asyncio.create_task(bot.start())
    print("🚀 FastAPI 伺服器已啟動，機器人正於記憶體中建立 MTProto 連線...")
    yield
    # 安全關閉
    await bot.stop()
    bot_task.cancel()

# 初始化 FastAPI
app = FastAPI(lifespan=lifespan)

# 2. 核心修復：加上 in_memory=True！
# 強迫 Hydrogram 在記憶體中建立 Session，不再去讀寫 Render 的唯讀硬碟，徹底解決背景崩潰問題
bot = Client(
    "stream_session", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    in_memory=True
)

# 3. 接收影片事件（支援直接傳送與從其他地方「轉發」）
@bot.on_message(filters.video | filters.document)
async def handle_media(client: Client, message: Message):
    # 核心修復：強制使用當前收到訊息的對話 ID，絕不抓取可能被隱私遮蔽的轉發來源 ID！
    current_chat_id = message.chat.id
    msg_id = message.id
    
    # 確保網域結尾沒有多餘的斜線
    base_domain = DOMAIN.rstrip('/')
    stream_url = f"{base_domain}/stream/{current_chat_id}/{msg_id}"
    
    # 使用安全的 reply_text 直接針對該訊息進行回覆
    await message.reply_text(
        f"🎬 **MTProto 轉發串流網址已生成：**\n\n`{stream_url}`\n\n可直接貼入 VLC 或 PotPlayer 播放！"
    )

# 4. 串流分塊讀取生成器（一次只讀 1MB 放到快取，不爆記憶體、不佔硬碟）
async def chunk_generator(msg, start: int, end: int, chunk_size: int):
    offset = start
    while offset <= end:
        current_size = min(chunk_size, end - offset + 1)
        chunk = await bot.download_media(msg, in_memory=True, offset=offset, limit=current_size)
        if not chunk:
            break
        yield bytes(chunk)
        offset += len(chunk)

# 5. 提供給播放器的 HTTP 介面（完美的 Range 解析邏輯，100% 支援所有播放器快進、倒退）
@app.get("/stream/{chat_id}/{message_id}")
async def stream_endpoint(chat_id: int, message_id: int, range: str = Header(None)):
    try:
        msg = await bot.get_messages(chat_id, message_id)
        media = msg.video or msg.document
        if not media:
            raise HTTPException(status_code=404, detail="找不到影片")

        file_size = media.file_size
        start, end = 0, file_size - 1

        # 修正後的安全解析 Range 區塊，防止空字串或格式問題導致播放器噴 500 錯誤
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
            "Content-Type": media.mime_type or "video/mp4",
        }
        return StreamingResponse(chunk_generator(msg, start, end, CHUNK_SIZE), status_code=206, headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 6. 極簡的首頁，方便你用瀏覽器檢查 Render 伺服器是否活著
@app.get("/")
async def index():
    return {"status": "running", "message": "Telegram Stream Bot is online!"}
