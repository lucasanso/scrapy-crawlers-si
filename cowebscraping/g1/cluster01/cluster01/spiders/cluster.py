import scrapy

from ..portals import PORTAL_CLASSES

KEYWORDS = ['grupo paramilitar']

class ClusterSpider(scrapy.Spider):
    name = 'cluster'
    allowed_domains = [portal.DOMAIN for portal in PORTAL_CLASSES.values()]
    # allowed_domains = ['cartacapital.com.br', 'lostiempos.com', 'laprensa.com.ar']

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)

        newspaper = kwargs.get('n')
        self.portal = PORTAL_CLASSES[newspaper]

    def start_requests(self):
        for keyword in KEYWORDS:
            page = self.portal.FIRST_PAGE
            url = self.portal.build_search_url(keyword)
            yield scrapy.Request(url, meta={'keyword': keyword, 'page': page})

    def parse(self, response):
        keyword = response.meta['keyword']

        for news_url in self.portal.get_news_urls(response):
            yield scrapy.Request(news_url, meta={'keyword': keyword}, callback=self.parse_news)

        if self.portal.has_next_page(response):
            next_page = response.meta['page'] + 1
            url = self.portal.build_search_url(keyword, next_page)
            yield scrapy.Request(url, meta={'keyword': keyword, 'page': next_page})

    def parse_news(self, response):
        item = self.portal.parse_news(response)
        if item: # do contrário é None e é uma notícia com paywall
            yield item