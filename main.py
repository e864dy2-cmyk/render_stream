import os
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from hydrogram import Client, filters
from hydrogram.types import Message

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "http://localhost:8000")

app = FastAPI()
bot = Client("stream_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@app.on_event("startup")
async def startup():
    await bot.start()

@app.on_event("shutdown")
async def shutdown():
    await bot.stop()

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
            parts = range.replace("bytes=", "").split("-")
            start = int(parts[0]) if parts[0] else 0
            end = int(parts[1]) if parts[1] else end

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
