# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class NewsItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field()
    acquisition_date = scrapy.Field()
    publication_date = scrapy.Field()
    newspaper = scrapy.Field()
    author = scrapy.Field()
    article = scrapy.Field()
    accepted_by = scrapy.Field()
    gangs = scrapy.Field()
    keyword = scrapy.Field()
    id_event = scrapy.Field()
