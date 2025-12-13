import json
import yaml
import sys
import pymongo
from scrapy.exceptions import IgnoreRequest
from scrapy import signals
from sshtunnel import open_tunnel
from itemadapter import is_item, ItemAdapter

# Tenta carregar as configurações no início
try:
    with open('config.yaml', 'r') as configs_file:
        configs = yaml.safe_load(configs_file)
except FileNotFoundError:
    print("ERRO CRÍTICO (Middleware): Arquivo config.yaml não encontrado!")
    configs = None


class WebScrapingNewsSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        pass

    def process_start_requests(self, start_requests, spider):
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class WebScrapingNewsDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        return None

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class DuplicateFilterMiddleware:
    def __init__(self, output_mode='json'):
        self.visited_urls = set()
        self.output_mode = output_mode

    @classmethod
    def from_crawler(cls, crawler):
        output_mode = crawler.settings.get('OUTPUT_MODE', 'json')
        middleware = cls(output_mode)
        crawler.signals.connect(middleware.open_spider, signal=signals.spider_opened)
        return middleware

    def open_spider(self, spider):
        spider.logger.info("Inicializando Filtro de Duplicatas...")
        
        # --- MODO JSON ---
        if self.output_mode == 'json':
            try:
                with open('visited_urls.json', 'r', encoding='utf-8') as f:
                    for line in f:
                        url_data = json.loads(line.strip())
                        self.visited_urls.add(url_data['url'])
            except FileNotFoundError:
                pass 

        # --- MODO DATABASE (COM SSH) ---
        elif self.output_mode == 'database':
            if not configs:
                spider.logger.error("Configuração não carregada. Pulo do filtro.")
                return

            lamcad_configs = configs['lamcad']
            mongo_configs = configs['mongodb_lamcad']
            
            server = None
            client = None
            
            # --- AQUI ESTÁ A CORREÇÃO DE PORTA ---
            # Usamos 27019 para ler o histórico, enquanto o Pipeline usa 27018 para salvar.
            porta_verificacao = 27019 

            try:
                # 1. Abre Túnel Temporário na porta 27019
                server = open_tunnel(
                    (lamcad_configs['server_ip'], lamcad_configs['server_port']),
                    ssh_username=lamcad_configs['ssh_username'],
                    ssh_password=lamcad_configs['ssh_password'],
                    local_bind_address=('127.0.0.1', porta_verificacao), # <--- Porta Alternativa
                    remote_bind_address=(lamcad_configs['remote_bind_ip'], lamcad_configs['remote_bind_port'])
                )
                server.start()
                
                # 2. Conecta ao Mongo usando a porta 27019
                client = pymongo.MongoClient(
                    host='127.0.0.1',
                    port=porta_verificacao,  # <--- Conecta na Porta Alternativa
                    serverSelectionTimeoutMS=5000
                )

                db = client[mongo_configs['database']]
                collection = db['visitedUrls']
                
                # 3. Lógica de Busca (Isso faltava no seu snippet!)
                newspaper = getattr(spider, 'article_newspaper_selector', None)
                query = {"newspaper": newspaper} if newspaper else {}
                
                # Traz apenas o campo URL para ser rápido
                cursor = collection.find(query, {"url": 1}) 
                
                count = 0
                for doc in cursor:
                    self.visited_urls.add(doc.get('url'))
                    count += 1
                
                spider.logger.info(f"{count} URLs carregadas do MongoDB para o filtro.")

            except Exception as e:
                spider.logger.error(f"Erro ao carregar duplicatas do Mongo: {e}")
            
            finally:
                # 4. Fecha tudo imediatamente
                if client: client.close()
                if server: server.stop()

    def process_request(self, request, spider):
        # Lógica de ignorar URL
        is_search_url = False
        if hasattr(spider, 'is_search_url'):
            is_search_url = spider.is_search_url(request.url)

        if not is_search_url and request.url in self.visited_urls:
            spider.logger.info(f"🚫 URL Duplicada ignorada: {request.url}")
            raise IgnoreRequest(f"🚫 URL já visitada: {request.url}")
        
        return None

    def process_response(self, request, response, spider):
        # Adiciona na memória local para evitar loops na mesma execução
        is_search_url = False
        if hasattr(spider, 'is_search_url'):
            is_search_url = spider.is_search_url(request.url)
            
        if not is_search_url:
            self.visited_urls.add(request.url)
            
        return response
