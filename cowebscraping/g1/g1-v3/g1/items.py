# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class G1Item(scrapy.Item):
    keyword = scrapy.Field()
    acquisition_date = scrapy.Field()
    publication_date = scrapy.Field()
    last_update = scrapy.Field()
    newspaper = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    article = scrapy.Field()
    tags = scrapy.Field()
    accepted_by = scrapy.Field()
    gangs = scrapy.Field()
    manual_relevance_class = scrapy.Field()
    automatic_relevance_class = scrapy.Field()
    relevance_model = scrapy.Field()
    certainty_level = scrapy.Field()
    relevance_classification_date = scrapy.Field()
    id_event = scrapy.Field()
    confidence_relevance_class = scrapy.Field()
    resumo = scrapy.Field()
    data_evento = scrapy.Field()
    quant_mortes = scrapy.Field()
    tipo_droga = scrapy.Field()
    quant_droga = scrapy.Field()
    pais = scrapy.Field()
    regiao = scrapy.Field()
    municipio = scrapy.Field()
    coordenados_GPS = scrapy.Field()
    tipo_conflito_armado = scrapy.Field()
    ator1_nome = scrapy.Field()
    ator1_cod = scrapy.Field()
    ator2_nome = scrapy.Field()
    ator2_cod = scrapy.Field()
    tipo_relacao_entre_atores = scrapy.Field()