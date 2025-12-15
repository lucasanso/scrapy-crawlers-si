import scrapy
from datetime import datetime, timedelta
from urllib.parse import quote, parse_qs, urlparse
from sshtunnel import open_tunnel
import pytz
import pymongo
import re
import yaml
from unidecode import unidecode
import sys
import os  # Necessário para verificar existência do arquivo de checkpoint

# Playwright
from scrapy_playwright.page import PageMethod

from ..items import G1Item
from ..keywords import SEARCH_KEYWORDS, VALIDATION_KEYWORDS, SEARCH_KEYWORDS_CHUNKS

# --- CONFIGURAÇÕES GLOBAIS ---
ORDER = 'recent'
SPECIES = quote('notícias')
SEARCH_DATE_FORMAT = r'%Y-%m-%d'
PAGE_SEARCH_URL_TEMPLATE = 'https://g1.globo.com/busca/?q={}&order={}&from={}T00%3A00%3A00-0300&to={}T23%3A59%3A59-0300&species={}'
CHECKPOINT_FILE = 'checkpoints.yaml'

def build_page_search_url(keyword, date):
    day_str = date.strftime(SEARCH_DATE_FORMAT)
    return PAGE_SEARCH_URL_TEMPLATE.format(quote(keyword), ORDER, day_str, day_str, SPECIES)

def get_seen_urls_from_mongodb(load_unaccepted=True):
    """
    Conecta ao MongoDB via SSH e retorna uma lista de URLs que já existem no banco.
    """
    print("\n🔄 [INICIALIZAÇÃO] Conectando ao Banco para carregar histórico...")
    
    seen_urls = []
    server = None
    client = None

    try:
        with open('config.yaml', 'r') as f:
            configs = yaml.safe_load(f)
        
        lc = configs['lamcad']
        mg = configs['mongodb_lamcad']
        
        server = open_tunnel(
            (lc['server_ip'], lc['server_port']),
            ssh_username=lc['ssh_username'],
            ssh_password=lc['ssh_password'],
            local_bind_address=(lc['local_bind_ip'], lc['local_bind_port']),
            remote_bind_address=(lc['remote_bind_ip'], lc['remote_bind_port'])
        )
        server.start()

        client = pymongo.MongoClient(mg['uri'])
        db = client[mg['database']]
        
        # 1. Carrega ACEITAS
        accepted_col = db[mg['accepted_news_collection']]
        accepted_urls = [doc['url'] for doc in accepted_col.find({}, {'url': 1})]
        print(f"   -> Encontradas {len(accepted_urls)} notícias ACEITAS.")
        seen_urls.extend(accepted_urls)

        # 2. Carrega RECUSADAS (Se solicitado)
        if load_unaccepted:
            unaccepted_col = db[mg['unaccepted_news_collection']]
            unaccepted_urls = [doc['url'] for doc in unaccepted_col.find({}, {'url': 1})]
            print(f"   -> Encontradas {len(unaccepted_urls)} notícias RECUSADAS.")
            seen_urls.extend(unaccepted_urls)
        
        print(f"✅ [SUCESSO] Total de {len(seen_urls)} URLs carregadas na memória.\n")
        
    except Exception as e:
        print(f"⚠️ [ERRO] Falha ao carregar histórico do banco: {e}")
        print("   -> O crawler vai iniciar zerado.")
    finally:
        if client: client.close()
        if server: server.stop()
    
    return set(seen_urls)

class ScrapeSpider(scrapy.Spider):
    name = "scrape"
    allowed_domains = ["g1.globo.com", "globo.com"]

    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 20000},
        'CONCURRENT_REQUESTS': 4,
        # Força o uso do Pipeline correto
        'ITEM_PIPELINES': {
            'g1.pipelines.MongoDBPipeline': 300,
        }
    }

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.items = [] 
        
        is_recheck = kwargs.get('recheck') == 'True'
        self.seen_urls = get_seen_urls_from_mongodb(load_unaccepted=not is_recheck)
        
        # Carrega todas as keywords alvo
        raw_keywords = []
        if kwargs.get('k'): raw_keywords = [kwargs.get('k')]
        elif kwargs.get('c'): raw_keywords = SEARCH_KEYWORDS_CHUNKS[int(kwargs.get('c'))]
        else: raw_keywords = SEARCH_KEYWORDS

        # --- LÓGICA DE CHECKPOINT (Filtro) ---
        # Se NÃO for recheck, removemos as keywords que já foram concluídas
        if not is_recheck:
            completed_keywords = self.load_checkpoints()
            self.keywords = [k for k in raw_keywords if k not in completed_keywords]
            skipped_count = len(raw_keywords) - len(self.keywords)
            if skipped_count > 0:
                print(f"⏩ [CHECKPOINT] Pulando {skipped_count} palavras-chave já concluídas anteriormente.")
        else:
            self.keywords = raw_keywords

        self.target_year = int(kwargs.get('y')) if kwargs.get('y') else 2024
        
        print(f"--- SPIDER PRONTO: {len(self.keywords)} palavras-chave restantes para processar ---")

    def start_requests(self):
        # --- DEFINIÇÃO DO SCRIPT DE SCROLL ---
        # Este script em JS vai rodar no navegador (Playwright).
        # Ele desce a página, espera carregar, e verifica se a altura aumentou.
        scroll_script = """
            async () => {
                let lastHeight = document.body.scrollHeight;
                while (true) {
                    window.scrollTo(0, document.body.scrollHeight);
                    // Espera 2 segundos para o conteúdo carregar (ajuste se necessário)
                    await new Promise(resolve => setTimeout(resolve, 2000));
                    
                    let newHeight = document.body.scrollHeight;
                    // Se a altura não mudou após o scroll e a espera, chegamos ao fim
                    if (newHeight === lastHeight) {
                        break;
                    }
                    lastHeight = newHeight;
                }
            }
        """

        start_date = datetime(self.target_year, 1, 1)
        end_date = datetime.now() if self.target_year == datetime.now().year else datetime(self.target_year, 12, 31)

        for keyword in self.keywords:
            self.logger.info(f"🚀 INICIANDO KEYWORD: {keyword}")
            curr = start_date
            while curr <= end_date:
                url = build_page_search_url(keyword, curr)
                
                meta = {
                    'keyword': keyword, 'date': curr,
                    'playwright': True, 'playwright_include_page': True,
                    'playwright_page_methods': [
                        PageMethod("wait_for_selector", "ul.results__list", timeout=15000),
                        # Agora a variável scroll_script existe e contém o código JS
                        PageMethod("evaluate", scroll_script),
                        # Uma espera final de segurança
                        PageMethod("wait_for_timeout", 1000), 
                    ]
                }
                
                yield scrapy.Request(url, self.parse_results_page, meta=meta, errback=self.errback_close, dont_filter=True)
                curr += timedelta(days=1)
            
            # --- SALVAR CHECKPOINT ---
            self.save_checkpoint(keyword)
            self.logger.info(f"💾 CHECKPOINT SALVO: '{keyword}' marcada como concluída.")

    # --- MÉTODOS DE CHECKPOINT ---
    def load_checkpoints(self):
        """Lê o arquivo YAML e retorna uma lista de keywords já finalizadas."""
        if not os.path.exists(CHECKPOINT_FILE):
            return []
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = yaml.safe_load(f)
                if data and 'completed_keywords' in data:
                    return data['completed_keywords']
        except Exception as e:
            print(f"⚠️ Erro ao ler checkpoint: {e}")
        return []

    def save_checkpoint(self, keyword):
        """Adiciona uma keyword ao arquivo YAML de checkpoints."""
        completed = self.load_checkpoints()
        if keyword not in completed:
            completed.append(keyword)
            try:
                with open(CHECKPOINT_FILE, 'w') as f:
                    yaml.dump({'completed_keywords': completed}, f)
            except Exception as e:
                self.logger.error(f"Erro ao salvar checkpoint: {e}")

    async def parse_results_page(self, response):
        page = response.meta["playwright_page"]
        try:
            links = response.css("li.widget--card a.widget--info__media::attr(href)").getall() or \
                    response.css("li.widget--card a.widget--info__text-container::attr(href)").getall()
            
            clean_links = []
            for l in links:
                if 'u=' in l:
                    try: l = parse_qs(urlparse(l).query)['u'][0]
                    except: pass
                clean_links.append(l)

            self.logger.info(f"[{response.meta['date'].strftime('%d/%m')}] KW: {response.meta['keyword']} - Encontrados: {len(clean_links)}")

            for url in clean_links:
                if url not in self.seen_urls:
                    news_meta = response.meta.copy()
                    news_meta.pop('playwright', None)
                    news_meta.pop('playwright_include_page', None)
                    news_meta.pop('playwright_page_methods', None)
                    
                    yield scrapy.Request(url, self.parse_news, meta=news_meta)
        finally:
            await page.close()

    async def errback_close(self, failure):
        if failure.request.meta.get("playwright_page"):
            await failure.request.meta["playwright_page"].close()

    def parse_news(self, response):
        if response.url in self.seen_urls: return

        title = response.css("h1.content-head__title::text").get() or response.css("h1.entry-title::text").get()
        if not title: return

        item = self.try_parse(response, self.parse_news_v1) or self.try_parse(response, self.parse_news_v2)

        if item:
            self.seen_urls.add(response.url)
            yield item

    def try_parse(self, response, method):
        try: return method(response)
        except: return None

    def extract_date(self, response):
        # 1. Tenta atributo datetime (ex: 2024-01-11T04:00...)
        iso_date = response.css('time[itemprop="datePublished"]::attr(datetime)').get()
        if iso_date:
            try:
                return datetime.strptime(iso_date[:10], r'%Y-%m-%d').strftime(r'%d-%m-%Y')
            except: pass

        # 2. Tenta texto visual V2
        text_v2 = response.css('time[itemprop="datePublished"]::text').get() or \
                  response.css('.content-publication-data__updated time::text').get()
        if text_v2:
            try: return datetime.strptime(text_v2.strip(), r'%d/%m/%Y %Hh%M').strftime(r'%d-%m-%Y')
            except: pass

        # 3. Tenta texto visual V1
        text_v1 = response.css('abbr.published::text').get()
        if text_v1:
            try: return datetime.strptime(text_v1.strip(), r'%d/%m/%Y %Hh%M').strftime(r'%d-%m-%Y')
            except: pass

        return None

    def _base_item(self, response, article, date_obj=None):
        item = G1Item()
        item['url'] = response.url
        item['keyword'] = response.meta['keyword']
        
        accepted = self.accept_article(article)
        item['accepted_by'] = accepted 

        if accepted:
            item['acquisition_date'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime(r'%d-%m-%Y')
            item['newspaper'] = 'G1'
            item['title'] = response.css("h1.content-head__title::text").get() or response.css("h1.entry-title::text").get()
            item['article'] = article
            item['gangs'] = self.search_gangs(article)
            item['publication_date'] = date_obj 
            item['id_event'] = None 
        
        return item

    def parse_news_v1(self, response):
        sub = ' '.join(response.css('h2::text').getall())
        art = sub + ' ' + ' '.join(response.css("div#materia-letra p::text").getall()).strip()
        dt = self.extract_date(response)
        return self._base_item(response, art, dt)

    def parse_news_v2(self, response):
        art = ' '.join(response.css("article[itemprop='articleBody'] p::text").getall())
        sub = response.css("h2[itemprop='alternativeHeadline']::text").get() or ""
        full_art = sub + " " + art
        dt = self.extract_date(response)
        return self._base_item(response, full_art, dt)

    def search_gangs(self, art):
        if not art: return []
        found = []
        for p in VALIDATION_KEYWORDS['GANGS']:
            found += re.findall(p, unidecode(art), re.IGNORECASE)
        return found

    def accept_article(self, art):
        if not art: return False
        org = False
        for p in VALIDATION_KEYWORDS['GANGS'] + VALIDATION_KEYWORDS['ORGANIZED CRIME']:
            if re.findall(p, unidecode(art.lower()), re.IGNORECASE) or 'pcc' in unidecode(art.lower()).split():
                org = p; break
        act = False
        for p in VALIDATION_KEYWORDS['DRUGS'] + VALIDATION_KEYWORDS['ARMED INTERACTIONS']:
            if re.findall(p, unidecode(art.lower()), re.IGNORECASE):
                act = p; break
        return f"{org} - {act}" if (org and act) else False
