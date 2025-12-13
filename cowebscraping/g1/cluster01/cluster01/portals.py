from urllib.parse import urlparse, parse_qs
from datetime import datetime
from abc import ABC, abstractmethod
import pytz
import json

from .items import Cluster01Item
from .utils import accept_article, search_gangs

class PortalInterface(ABC):
    NAME = None
    DOMAIN = None
    SEARCH_URL = None
    FIRST_PAGE = None

    def __init__(self):
        super().__init__()
        self.__class__.check_class_methods()

    @classmethod
    def check_class_methods(cls):
        """Método que verifica se as classes que herdam desta definiram os atributos de
        classe necessários."""

        if not hasattr(cls, 'NAME') or not cls.NAME:
            raise AttributeError(f"{cls.__name__} must define 'NAME'.")
        if not hasattr(cls, 'DOMAIN') or not cls.DOMAIN:
            raise AttributeError(f"{cls.__name__} must define 'DOMAIN'.")
        if not hasattr(cls, 'SEARCH_URL') or not cls.SEARCH_URL:
            raise AttributeError(f"{cls.__name__} must define 'SEARCH_URL'.")
        if not hasattr(cls, 'FIRST_PAGE') or not cls.FIRST_PAGE:
            raise AttributeError(f"{cls.__name__} must define 'FIRST_PAGE'.")

    @abstractmethod
    def get_news_urls(response) -> list:
        ...

    @abstractmethod
    def has_next_page(response) -> bool:
        ...

    @abstractmethod
    def build_search_url(keyword, page=FIRST_PAGE) -> str:
        ...

    @abstractmethod
    def parse_news(response):
        ...

class CartaCapital(PortalInterface):
    NAME = 'CartaCapital'
    DOMAIN = 'cartacapital.com.br'
    SEARCH_URL = 'https://www.cartacapital.com.br/page/{page}/?s={keyword}'
    FIRST_PAGE = 1

    def get_news_urls(response):
        return response.css('a.l-list__item::attr(href)').getall()
    
    def has_next_page(response):
        return response.xpath('//span[text()="Próxima"]').get()
    
    def build_search_url(keyword, page=FIRST_PAGE):
        return CartaCapital.SEARCH_URL.format(keyword=keyword.replace(' ', '+'), page=page)

    def parse_news(response):
        title = response.css('h1::text').get()
        subtitle = response.xpath("//section[@class='s-content__heading']/p[2]/text()").get()
        article = response.css('section.contentsingle').xpath('string(.)').extract_first()

        if not subtitle: # notícia com paywall
            return None
        
        article = ' '.join([subtitle, article])

        item = Cluster01Item()
        accepted = accept_article(' '.join([title, article]))
        if accepted:
            item['keyword'] = response.meta['keyword']
            item['acquisition_date'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime(r'%d-%m-%Y')
            item['publication_date'] = datetime.fromisoformat(response.css("meta[property='article:published_time']::attr(content)").get()).strftime(r'%d-%m-%Y')
            last_update_el = response.css("meta[property='article:modified_time']::attr(content)").get()
            item['last_update'] = datetime.fromisoformat(last_update_el).strftime(r'%d-%m-%Y') if last_update_el else None
            item['newspaper'] = CartaCapital.NAME
            item['title'] = title
            item['article'] = article
            item['tags'] = response.css("meta[property='article:tag']::attr(content)").getall() + [response.css("meta[property='article:section']::attr(content)").get()]
            item['accepted_by'] = accepted
            item['gangs'] = search_gangs(' '.join([title, article]))
        item['url'] = response.url
        return item

class LosTiempos(PortalInterface):
    NAME = 'Los Tiempos'
    SEARCH_URL = 'https://www.lostiempos.com/hemeroteca?contenido={keyword}&page={page}'
    FIRST_PAGE = 0

    def __init__(self):
        super().__init__()

    def get_news_urls(response):
        return ['https://www.lostiempos.com' + relative_url
                for relative_url in response.css('div.views-row div.views-field-title a::attr(href)').getall()[:10]]
    
    def has_next_page(response):
        return response.css('div.view-busqueda li.pager-next a').get()
    
    def build_search_url(keyword, page=FIRST_PAGE):
        pass

class LaPrensa(PortalInterface):
    NAME = 'La Prensa'
    SEARCH_URL = 'https://www.laprensa.com.ar/json/apps/notes.aspx?allfields={keyword}&pagesize=50&page={page}'
    FIRST_PAGE = 1

    def __init__(self):
        super().__init__()

    def get_news_urls(response):
        json_data = json.loads(response.body)
        return [news['link'] for news in json_data['notes']]
    
    def has_next_page(response):
        total_results = json.loads(response.body)['t']
        parsed_url = parse_qs(urlparse(response.request.url).query)
        page_size = int(parsed_url['pagesize'][0])
        page = int(parsed_url['page'][0])
        return (total_results - (page_size*page)) > 0
    
    def build_search_url(keyword, page=FIRST_PAGE):
        pass

PORTAL_CLASSES = {
    'CartaCapital': CartaCapital,
    'Los Tiempos': LosTiempos,
    'La Prensa': LaPrensa,
}
