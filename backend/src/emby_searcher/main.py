from fastapi import FastAPI

app = FastAPI(title="Emby Searcher")


@app.get("/health")
async def health():
    return {"status": "ok"}
