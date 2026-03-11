"""Elasticsearch index management and bulk operations for Emby movies."""

from elasticsearch import AsyncElasticsearch

from emby_searcher.config import get_elasticsearch_url

INDEX_NAME = "emby_movies"

INDEX_SETTINGS = {
    "mappings": {
        "properties": {
            "emby_id": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "standard"},
            "sort_name": {"type": "keyword"},
            "year": {"type": "integer"},
            "overview": {"type": "text"},
            "genres": {"type": "keyword"},
            "studios": {"type": "keyword"},
            "community_rating": {"type": "float"},
            "official_rating": {"type": "keyword"},
            "imdb_id": {"type": "keyword"},
            "tmdb_id": {"type": "keyword"},
        }
    }
}


def get_client() -> AsyncElasticsearch:
    """Create an async Elasticsearch client."""
    return AsyncElasticsearch(get_elasticsearch_url())


def _extract_provider_ids(movie: dict) -> dict:
    """Extract provider IDs from a movie dict."""
    return movie.get("ProviderIds") or {}


def _extract_studio_names(movie: dict) -> list[str]:
    """Extract studio name strings from a movie dict."""
    studios = movie.get("Studios") or []
    return [s.get("Name", "") for s in studios]


def to_doc(movie: dict) -> dict:
    """Convert an Emby movie item to an Elasticsearch document."""
    provider_ids = _extract_provider_ids(movie)
    return {
        "emby_id": str(movie["Id"]),
        "name": movie.get("Name", ""),
        "sort_name": movie.get("SortName", ""),
        "year": movie.get("ProductionYear"),
        "overview": movie.get("Overview", ""),
        "genres": movie.get("Genres", []),
        "studios": _extract_studio_names(movie),
        "community_rating": movie.get("CommunityRating"),
        "official_rating": movie.get("OfficialRating"),
        "imdb_id": provider_ids.get("Imdb"),
        "tmdb_id": provider_ids.get("Tmdb"),
    }


def _build_upsert_pair(doc: dict) -> list[dict]:
    """Build a bulk update action + document pair."""
    action = {"update": {"_index": INDEX_NAME, "_id": doc["emby_id"]}}
    body = {"doc": doc, "doc_as_upsert": True}
    return [action, body]


def _build_bulk_operations(movies: list[dict]) -> list[dict]:
    """Build bulk operations list from movies."""
    operations: list[dict] = []
    for movie in movies:
        operations.extend(_build_upsert_pair(to_doc(movie)))
    return operations


def _count_failures(response: dict) -> int:
    """Count failed items in a bulk response."""
    return sum(1 for item in response["items"] if item["update"].get("error"))


async def ensure_index(es: AsyncElasticsearch) -> None:
    """Create the movies index if it doesn't exist."""
    exists = await es.indices.exists(index=INDEX_NAME)
    if not exists:
        await es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)


async def bulk_upsert(es: AsyncElasticsearch, movies: list[dict]) -> int:
    """Upsert all movies into Elasticsearch. Returns count of successful upserts."""
    operations = _build_bulk_operations(movies)
    response = await es.bulk(operations=operations)
    return len(movies) - _count_failures(response)
