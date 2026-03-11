"""Client for fetching movie data from the Emby API."""

import httpx

from emby_searcher.config import get_emby_api_key, get_emby_url

PAGE_SIZE = 250
FIELDS = "Overview,Genres,ProviderIds,Studios,CommunityRating,OfficialRating"


def _build_params(start_index: int) -> dict:
    """Build query params for an Emby Items request."""
    return {
        "IncludeItemTypes": "Movie",
        "Recursive": "true",
        "Fields": FIELDS,
        "StartIndex": start_index,
        "Limit": PAGE_SIZE,
        "SortBy": "SortName",
        "SortOrder": "Ascending",
    }


def _build_headers() -> dict:
    """Build auth headers for Emby."""
    return {"X-Emby-Token": get_emby_api_key()}


def _items_url() -> str:
    """Build the Emby Items endpoint URL."""
    return f"{get_emby_url().rstrip('/')}/Items"


async def _fetch_page(client: httpx.AsyncClient, start_index: int) -> dict:
    """Fetch a single page of movies from Emby."""
    response = await client.get(
        _items_url(),
        params=_build_params(start_index),
        headers=_build_headers(),
    )
    response.raise_for_status()
    return response.json()


def _extract_items(page: dict) -> list[dict]:
    """Extract the Items list from a page response."""
    return page.get("Items", [])


def _extract_total(page: dict) -> int:
    """Extract TotalRecordCount from a page response."""
    return page.get("TotalRecordCount", 0)


def _page_offsets(total: int) -> list[int]:
    """Generate start indexes for all pages."""
    return list(range(0, total, PAGE_SIZE))


async def fetch_all_movies() -> list[dict]:
    """Fetch all movies from Emby, paginating through results."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        first_page = await _fetch_page(client, 0)
        total = _extract_total(first_page)
        movies = _extract_items(first_page)

        remaining_offsets = _page_offsets(total)[1:]
        for offset in remaining_offsets:
            page = await _fetch_page(client, offset)
            movies.extend(_extract_items(page))

    return movies
