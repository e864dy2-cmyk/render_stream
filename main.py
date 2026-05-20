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
    # 背景啟動機器人，避免卡死 FastAPI
    bot_task = asyncio.create_task(bot.start())
    print("🚀 FastAPI 已啟動，機器人正於記憶體中建立 MTProto 連線...")
    yield
    await bot.stop()
    bot_task.cancel()

app = FastAPI(lifespan=lifespan)

# 💡 關鍵修復：加上 in_memory=True！
# 強迫 Hydrogram 在記憶體中建立 Session，不再去讀寫 Render 的唯讀硬碟，徹底解決背景崩潰問題
bot = Client(
    "stream_session", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    in_memory=True
)

@bot.on_message(filters.video | filters.document)
async def handle_media(client: Client, message: Message):
    chat_id = message.chat.id
    msg_id = message.id
    stream_url = f"{DOMAIN}/stream/{chat_id}/{msg_id}"
    await message.reply_text(f"🎬 **MTProto 串流網址已生成：**\n\n`{stream_url}`\n\n可直接貼入播放器播放！")

async def chunk_generator(msg, start: int, end: int, chunk_size: int):
    offset = start
    while offset <= end:
        current_size = min(chunk_size, end - offset + 1)
        chunk = await bot.download_media(msg, in_memory=True, offset=offset, limit=current_size)
        if not chunk:
            break
        yield bytes(chunk)
        offset += len(chunk)

@app.get("/stream/{chat_id}/{message_id}")
async def stream_endpoint(chat_id: int, message_id: int, range: str = Header(None)):
    try:
        msg = await bot.get_messages(chat_id, message_id)
        media = msg.video or msg.document
        if not media:
            raise HTTPException(status_code=404, detail="找不到影片")

        file_size = media.file_size
        start, end = 0, file_size - 1

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

        CHUNK_SIZE = 1024 * 1024 
        
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": media.mime_type or "video/mp4",
        }
        return StreamingResponse(chunk_generator(msg, start, end, CHUNK_SIZE), status_code=206, headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def index():
    return {"status": "running", "message": "Telegram Stream Bot is online!"}
