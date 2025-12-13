import json
import yaml
import os
import re
from urllib.parse import urlparse, parse_qs, quote
import scrapy
from pymongo import MongoClient
from ..keyword_manager import KeywordManager
from ..items import NewsItem
from datetime import datetime
import pytz

class BaseSpider(scrapy.Spider):
    name = 'base_spider'
    allowed_domains = []
    custom_settings = {
        'HTTPERROR_ALLOWED_CODES': [404],
        'HTTPERROR_ALLOW_ALL': True,
    }

    # Seletores padrão
    search_url_template = ''
    search_results_selector = ''
    next_page_selector = ''
    article_title_selector = ''
    article_date_selector = ''
    article_author_selector = ''
    article_content_selector = ''
    article_newspaper_selector = ''
    payed_articles_selector = ''

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(crawler.settings, *args, **kwargs)
        spider._set_crawler(crawler)
        return spider
    
    @property
    def checkpoint_filename(self):
        """Gera um nome de arquivo único para cada spider (ex: completed_keywords_spidername.yaml)."""
        return f"completed_keywords_{self.name}.yaml"

    def __init__(self, settings, keyword=None, continue_scraping=False, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self.settings = settings
        self.continue_scraping = continue_scraping
        self.outstanding_requests = 0
        self.keyword_index = 0
        self.current_keyword = None
        self.stop_url = None
        self.stop_keyword = None
        self.keyword_manager = None
        self.search_keywords = None
        self.validation_keywords = None
        self.user_keyword = keyword
        
        self.initialize_keywords()

    # -------------------------------------------------------------------------
    # MÉTODOS DE CHECKPOINT (YAML)
    # -------------------------------------------------------------------------
    def get_ignored_keywords(self):
        """Lê o arquivo YAML e retorna um CONJUNTO (set) de palavras para performance."""
        filename = self.checkpoint_filename
        
        if not os.path.exists(filename):
            return set()
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                return set(data) if isinstance(data, list) else set()
        except Exception as e:
            self.logger.error(f"⚠️ Erro ao ler checkpoint YAML ({filename}): {e}")
            return set()

    def mark_as_done(self, keyword):
        """Salva a palavra no arquivo YAML de forma segura."""
        if not keyword: return

        filename = self.checkpoint_filename
        
        # 1. Carrega a lista atual (lê como set para evitar duplicatas, converte para lista para salvar)
        current_set = self.get_ignored_keywords()
        
        if keyword not in current_set:
            current_list = list(current_set) # YAML precisa de lista
            current_list.append(keyword)
            
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    # Salva em formato de lista bonita (- item)
                    yaml.dump(current_list, f, allow_unicode=True, default_flow_style=False)
                self.logger.info(f"💾 Checkpoint YAML: '{keyword}' salvo em {filename}")
            except Exception as e:
                self.logger.error(f"❌ Erro ao salvar YAML: {e}")

    def initialize_keywords(self):
        # --- GARANTIA DE INICIALIZAÇÃO DO MANAGER ---
        if self.keyword_manager is None:
            if self.continue_scraping:
                self.logger.info("Tentando recuperar estado anterior...")
                self.stop_url = self.get_last_url()
                keyword_atual = self.extract_keyword_from_url(self.stop_url) if self.stop_url else None
                self.keyword_manager = KeywordManager(self.user_keyword, self.continue_scraping, keyword_atual)
            else:
                self.keyword_manager = KeywordManager(keyword=self.user_keyword)

        # 1. Carrega TODAS as palavras
        full_list = self.keyword_manager.get_search_keywords()
        self.validation_keywords = self.keyword_manager.validation_keywords

        # -----------------------------------------------------------
        # CORREÇÃO AQUI: Mudamos os nomes para 'range_start' e 'range_end'
        # -----------------------------------------------------------
        
        # Tenta pegar 'range_start', se não vier, assume 0
        start_index = int(getattr(self, 'inicio', 0))
        
        # Tenta pegar 'range_end', se não vier, assume None
        end_index = getattr(self, 'fim', None)

        if end_index:
            end_index = int(end_index)
            self.search_keywords = full_list[start_index:end_index]
            self.logger.info(f"✂️ Recorte aplicado: índices [{start_index} a {end_index}]")
        else:
            self.search_keywords = full_list[start_index:]
            if start_index > 0:
                self.logger.info(f"✂️ Recorte aplicado: iniciando do índice {start_index}")

        # 3. Filtra o Checkpoint (YAML)
        done = self.get_ignored_keywords()
        slice_count = len(self.search_keywords)
        self.search_keywords = [k for k in self.search_keywords if k not in done]
        
        skipped = slice_count - len(self.search_keywords)
        
        self.logger.info(f"📋 Planejamento: {slice_count} palavras no intervalo selecionado.")
        if skipped > 0:
             self.logger.info(f"⏭️ Pulando {skipped} palavras já concluídas (Checkpoint YAML).")
        
        self.logger.info(f"🚀 Total a executar agora: {len(self.search_keywords)}")
        
        
    def start_requests(self):
        yield from self.process_next_keyword()

    def process_next_keyword(self):
        if self.keyword_index < len(self.search_keywords):
            self.current_keyword = self.search_keywords[self.keyword_index]
            self.keyword_index += 1
            if self.stop_url:
                search_url = self.stop_url
                self.logger.info(f"Iniciando busca a partir da url: {self.stop_url}")
                self.stop_url = None 
            else:
                search_url = self.construct_search_url(self.current_keyword)
                self.logger.info(f"Iniciando busca para a palavra-chave: {self.current_keyword}")
            self.outstanding_requests = 1
            yield scrapy.Request(url=search_url, callback=self.parse_search_results)
        else:
            self.logger.info("🏁 Todas as palavras-chave foram processadas.")

    def construct_search_url(self, keyword):
        return self.search_url_template.format(keyword=keyword.replace(' ', '+'))

    def parse_search_results(self, response):
        if response.status == 400:
            self.logger.info(f"Não foi possível acessar a página de pesquisa {response.url}")

        article_links = response.xpath(self.search_results_selector).getall()
        for link in article_links:
            full_link = response.urljoin(link)
            self.outstanding_requests += 1
        
            yield scrapy.Request(
                url=full_link, 
                callback=self.parse_item, 
                errback=self.handle_failure,
                dont_filter=True  # <--- ADICIONE ISSO (Obrigatório)
        )
            
        next_page = response.xpath(self.next_page_selector).get()
        if next_page:
            self.outstanding_requests += 1
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse_search_results)

        self.outstanding_requests -= 1
        if self.outstanding_requests == 0:
            yield from self.check_and_advance()

    def parse_item(self, response):
        # Verifica se o artigo é pago
        is_paid = response.xpath(self.payed_articles_selector).get()
        if is_paid:
            self.logger.info(f"Artigo pago detectado: {response.url}. Ignorando.")
            self.outstanding_requests -= 1
            if self.outstanding_requests == 0:
                yield from self.check_and_advance()
            return 
            
        self.logger.info(f"Extraindo notícia do link: {response.url}")
        
        item = NewsItem()
        
        date_iso = response.xpath(self.article_date_selector).get()
        item['publication_date'] = None
    
        if date_iso:
            try:
                date_iso_clean = date_iso.split('T')[0]
                date_obj = datetime.strptime(date_iso_clean, '%Y-%m-%d')
                item['publication_date'] = date_obj.strftime('%d-%m-%Y')
            except ValueError:
                self.logger.warning(f"Erro formato data: {date_iso}. Usando original.")
                item['publication_date'] = date_iso
        else:
            self.logger.warning(f"Data de publicação não encontrada com o seletor {self.article_date_selector} em {response.url}")

        item['title'] = response.xpath(self.article_title_selector).get()
        item['url'] = response.url
        item['acquisition_date'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime(r'%d-%m-%Y')
        item['author'] = response.xpath(self.article_author_selector).get()
        item['newspaper'] = self.article_newspaper_selector
        item['article'] = ' '.join(response.xpath(self.article_content_selector).getall()).strip()
        item['keyword'] = self.current_keyword

        gangs_found = self.keyword_manager.search_gangs(item)
        if gangs_found:
            item['gangs'] = gangs_found 

    
        accepted_keyword = self.keyword_manager.accept_article(item)
        if accepted_keyword:
            item['accepted_by'] = accepted_keyword
            item['gangs'] = self.keyword_manager.search_gangs(item)
            yield item
        else:
            self.logger.info(f"Artigo ignorado: não contém palavras-chave de validação - {item['url']}")
            item['accepted_by'] = None
            yield item

        self.outstanding_requests -= 1
        if self.outstanding_requests == 0:
            yield from self.check_and_advance()

    def handle_failure(self, failure):
        """
        Chamado quando ocorre um erro na requisição (404, DNS, Timeout)
        OU quando o Middleware ignora a requisição (IgnoreRequest).
        """
        self.outstanding_requests -= 1
        if self.outstanding_requests == 0:
            yield from self.check_and_advance()

    def check_and_advance(self):
        """
        Finaliza a palavra atual e avança.
        Nota: Não decrementa requests aqui, pois o caller já deve ter feito e verificado se é 0.
        """
        # Garante que não ficou negativo
        if self.outstanding_requests < 0:
            self.outstanding_requests = 0

        # Salva a palavra atual no YAML
        if self.current_keyword:
            self.logger.info(f"🎉 Extração finalizada com sucesso para: {self.current_keyword}")
            self.mark_as_done(self.current_keyword)
        
        # Reseta e chama a próxima
        self.outstanding_requests = 0 
        yield from self.process_next_keyword()
        
    
    def get_last_url(self):
        if not self.allowed_domains:
            self.logger.error("allowed_domains está vazio. Não é possível determinar o domínio.")
            return None

        domain = self.allowed_domains[0]
        
        if self.settings.get('OUTPUT_MODE') == 'database':
            self.logger.info("Buscando última URL no banco de dados")
            client = MongoClient(self.settings.get('MONGO_URI'))
            try:
                db = client[self.settings.get('MONGO_DATABASE')]
                entries = db['visitedUrls'].find({'url': {'$regex': f'.*busca\\?q={self.user_keyword.replace(" ", "+")}.*page=\\d+.*'}})
                urls = [entry['url'] for entry in entries]

                if not urls:
                    self.logger.warning(f"Nenhuma URL encontrada contendo a palavra-chave '{self.user_keyword}'.")
                    return None

                max_page_url = self._get_max_page_url(urls)
                self.logger.info(f"Última URL encontrada: {max_page_url}")
                return max_page_url
            finally:
                client.close()

        self.logger.warning("A saída configurada não é 'database'. Nenhuma URL será buscada.")
        return None

    def _get_max_page_url(self, urls):
        max_page_url = max(
            urls,
            key=lambda url: int(re.search(r'[?&]page=(\d+)', url).group(1)) if re.search(r'[?&]page=(\d+)', url) else 0,
            default=None
        )
        return max_page_url
        
    def extract_keyword_from_url(self, url):
        if not url:
            self.logger.error("URL inválida fornecida para extração de palavra-chave.")
            return None

        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        keyword = query_params.get('q', [None])[0]
        return keyword

    def is_search_url(self, url):
        if  '?s=' in url or '?q=' in url:
            return True
        else:
            return False
