DEFAULT_TOKEN = "a44711fa15a902e06f8643bbd3b957cb"
MOVIES_API_URL = "https://api.themoviedb.org/3"


class TheMovieDBScraper:
    def __init__(self, token: str = DEFAULT_TOKEN, *args, **kwargs):
        self.token = token

    def get_endpoint_url(self, url: str):
        sep = '?' if '?' not in url else '&'
        return f"{url}{sep}api_key={self.token}"
