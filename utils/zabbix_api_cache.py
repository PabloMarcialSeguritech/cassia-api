from cachetools import TTLCache

cache = TTLCache(maxsize=1, ttl=3500)


def store_token_in_cache(token):
    cache['token'] = token


def get_token_from_cache():
    return cache.get('token')
