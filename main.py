import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from hydrogram import Client, filters
from hydrogram.types import Message

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "http://localhost:8000")

@asynccontextmanager
async def lifespan(app: FastAPI):
    bot_task = asyncio.create_task(bot.start())
    print("🚀 FastAPI 已啟動，串流機器人已成功在記憶體中就緒！")
    yield
    await bot.stop()
    bot_task.cancel()

app = FastAPI(lifespan=lifespan)

bot = Client(
    "stream_session", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    in_memory=True
)

@bot.on_message(filters.video | filters.document)
async def handle_media(client: Client, message: Message):
    media = message.video or message.document
    if not media:
        return
        
    # 💡 核心進化：直接抓取檔案的唯一 ID (file_id)，繞過所有聊天室隱私與快取限制
    unique_file_id = media.file_id
    
    base_domain = DOMAIN.rstrip('/')
    stream_url = f"{base_domain}/stream/{unique_file_id}"
    
    await message.reply_text(
        f"🎬 **MTProto 串流網址已成功生成：**\n\n`{stream_url}`\n\n💡 提示：請完整複製此網址貼入 VLC / PotPlayer 即可播放！"
    )

# 串流分塊生成器：直接讀取 file_id 區塊
async def chunk_generator(file_id: str, start: int, end: int, chunk_size: int):
    offset = start
    while offset <= end:
        current_size = min(chunk_size, end - offset + 1)
        # 用 file_id 直接向 Telegram 伺服器請求分塊，100% 成功且極速不爆記憶體
        chunk = await bot.download_media(
            file_id, 
            in_memory=True, 
            offset=offset, 
            limit=current_size
        )
        if not chunk:
            break
        yield bytes(chunk)
        offset += len(chunk)

# 💡 串流路由更新：不再需要 chat_id 與 message_id
@app.get("/stream/{file_id}")
async def stream_endpoint(file_id: str, range: str = Header(None)):
    try:
        # 1. 透過 file_id 解析檔案的基本大小（不需要抓取整條訊息快取）
        file_properties = await bot.get_file(file_id)
        if not file_properties:
            raise HTTPException(status_code=404, detail="檔案不存在或已過期")
            
        file_size = file_properties.file_size
        start, end = 0, file_size - 1

        # 2. 解析播放器的 Range 快進/倒退請求
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
            "Content-Type": "video/mp4", # 強制指定為常見影片串流格式
        }
        return StreamingResponse(
            chunk_generator(file_id, start, end, CHUNK_SIZE), 
            status_code=206, 
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def index():
    return {"status": "running", "message": "Telegram Stream Bot is online!"}
