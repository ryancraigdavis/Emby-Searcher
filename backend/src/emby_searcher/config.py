import os


def get_env(key: str) -> str:
    value = os.environ.get(key)
    if value is None:
        raise ValueError(f"Missing required environment variable: {key}")
    return value


def get_emby_url() -> str:
    return get_env("EMBY_URL")


def get_emby_api_key() -> str:
    return get_env("EMBY_API_KEY")


def get_elasticsearch_url() -> str:
    return os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
