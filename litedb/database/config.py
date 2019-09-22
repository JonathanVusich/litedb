"""This class allows configuration of the page mechanism in liteDB."""


class Config:

    def __init__(self, page_size: int = 512, page_cache: int = 512):
        self._page_size = page_size
        self._page_cache = page_cache

    @property
    def page_size(self):
        return self._page_size

    @property
    def page_cache(self):
        return self._page_cache
