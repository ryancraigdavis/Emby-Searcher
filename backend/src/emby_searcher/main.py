from fastapi import FastAPI

from emby_searcher.sync import run_sync

app = FastAPI(title="Emby Searcher")


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/sync")
async def sync():
    """Trigger a full Emby -> Elasticsearch sync."""
    return await run_sync()
