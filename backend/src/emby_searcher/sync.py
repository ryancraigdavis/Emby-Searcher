"""Sync job: fetches all movies from Emby and upserts them into Elasticsearch."""

import asyncio
import logging

from emby_searcher.elastic import bulk_upsert, ensure_index, get_client
from emby_searcher.emby_client import fetch_all_movies

logger = logging.getLogger(__name__)


async def _sync_to_elasticsearch(movies: list[dict]) -> int:
    """Push movies into Elasticsearch and return success count."""
    es = get_client()
    await ensure_index(es)
    synced = await bulk_upsert(es, movies)
    await es.close()
    return synced


async def run_sync() -> dict:
    """Fetch all Emby movies and sync them to Elasticsearch."""
    movies = await fetch_all_movies()
    logger.info("Fetched %d movies from Emby", len(movies))

    synced = await _sync_to_elasticsearch(movies)
    logger.info("Synced %d/%d movies to Elasticsearch", synced, len(movies))
    return {"fetched": len(movies), "synced": synced}


def main() -> None:
    """CLI entrypoint for the sync job."""
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(run_sync())
    print(result)


if __name__ == "__main__":
    main()
