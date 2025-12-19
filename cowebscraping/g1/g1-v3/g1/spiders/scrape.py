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
from scrapy_playwright.page import PageMethod

from ..items import G1Item
from ..keywords import SEARCH_KEYWORDS, VALIDATION_KEYWORDS, SEARCH_KEYWORDS_CHUNKS

# Configurações globais.
ORDER = 'recent'
SPECIES = quote('notícias')
SEARCH_DATE_FORMAT = r'%Y-%m-%d'
PAGE_SEARCH_URL_TEMPLATE = 'https://g1.globo.com/busca/?q={}&order={}&from={}T00%3A00%3A00-0300&to={}T23%3A59%3A59-0300&species={}'
CHECKPOINT_FILE = 'checkpoints.yaml'


# Método complementar para bloquear medias como vídeo e imagem -> Poupar tempo e memória RAM a ser consumida durante o crawler.
def should_abort_request(request):
    return request.resource_type in ["image", "media", "font", "stylesheet"]


# Método que constrói a URL a ser pesquisada com cada uma das palavras-chave. Ex: "pcc"
def build_page_search_url(keyword, date):
    day_str = date.strftime(SEARCH_DATE_FORMAT)
    return PAGE_SEARCH_URL_TEMPLATE.format(quote(keyword), ORDER, day_str, day_str, SPECIES)


# Método que retorna a quantidade de notícias aceitas e não aceitas.
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
        
        # 1. Carregando ACEITAS
        accepted_col = db[mg['accepted_news_collection']]
        accepted_urls = [doc['url'] for doc in accepted_col.find({}, {'url': 1})]
        print(f"   -> Encontradas {len(accepted_urls)} notícias ACEITAS.")
        seen_urls.extend(accepted_urls)

        # 2. Carregando RECUSADAS
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


# Classe do spider G1
class ScrapeSpider(scrapy.Spider):
    name = "scrape"
    allowed_domains = ["g1.globo.com", "globo.com"]
    
    # Configurações do crawler
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 20000},             # headless : True faz com que não apareça o navegador simulado.
        'CONCURRENT_REQUESTS': 4,
        'PLAYWRIGHT_ABORT_REQUEST': should_abort_request, 
        'ITEM_PIPELINES': {
            'g1.pipelines.MongoDBPipeline': 300,
        }
    }
    
    
    # Método que inicia o crawler; Carrega as palavras-chave; Recebe as palavras-chave para passar como parâmetro [scrapy crawl scrape -a k="pcc" I scrapy crawl scrape -a c=1]
    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.items = [] 
        
        is_recheck = kwargs.get('recheck') == 'True'
        self.seen_urls = get_seen_urls_from_mongodb(load_unaccepted=not is_recheck)
        
        # Carrega todas as palavras-chave
        raw_keywords = []
        if kwargs.get('k'): raw_keywords = [kwargs.get('k')]
        elif kwargs.get('c'): raw_keywords = SEARCH_KEYWORDS_CHUNKS[int(kwargs.get('c'))]
        else: raw_keywords = SEARCH_KEYWORDS
    
        # Lógica de checkpoint
        if not is_recheck:
            completed_keywords = self.load_checkpoints()
            self.keywords = [k for k in raw_keywords if k not in completed_keywords]
            skipped_count = len(raw_keywords) - len(self.keywords)
            if skipped_count > 0:
                print(f"⏩ [CHECKPOINT] Pulando {skipped_count} palavras-chave já concluídas anteriormente.")
        else:
            self.keywords = raw_keywords

        self.target_year = int(kwargs.get('y')) if kwargs.get('y') else 2023
        
        print(f"--- SPIDER PRONTO: {len(self.keywords)} palavras-chave restantes para processar ---")


    # Método que inicia as requisições do Playwright para simular a navegação do navegador.
    def start_requests(self):
        # Logo abaixo tem-se um trecho de código em JavaScript para simular a navegação.
        # Lógica do código:
        
        # 1. Mede a página.
        # 2. Desce tudo.
        # 3. Espera 2 segundos.
        # 4. Mede de novo.
        # 5. Cresceu? Repete o processo.
        # 6. Não cresceu? Acabou, pode sair e coletar os links.
        
        
        scroll_script = """
            async () => {
                let lastHeight = document.body.scrollHeight;
                while (true) {
                    window.scrollTo(0, document.body.scrollHeight);
                    await new Promise(resolve => setTimeout(resolve, 2000));
                    
                    let newHeight = document.body.scrollHeight;
                    if (newHeight === lastHeight) {
                        break;
                    }
                    lastHeight = newHeight;
                }
            }
        """
        # Por que usar um script de JS? Porque o G1 possui rolagem infinita.
        # A ideia é rolar até encontrar a última notícia. Após encontrar o final da página, extrair cada uma das notícias do dia.
        
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
            
            self.save_checkpoint(keyword)
            self.logger.info(f"💾 PALAVRA-CHAVE '{keyword}' foi totalmente processada. Salvando no arquivo.")


    # Método que lê o arquivo .yaml e retorna uma lista das palavras-chave já finalizadas
    def load_checkpoints(self):
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


    # Método que adiciona a palvra-chave que foi totalmente processada no arquivo .yaml (checkpoint).
    def save_checkpoint(self, keyword):
        completed = self.load_checkpoints()
        if keyword not in completed:
            completed.append(keyword)
            try:
                with open(CHECKPOINT_FILE, 'w') as f:
                    yaml.dump({'completed_keywords': completed}, f)
            except Exception as e:
                self.logger.error(f"Erro ao salvar checkpoint: {e}")
    
    
    # Método que, para cada link, verifica se está no banco de dados [unaccepted], caso não estiver, chama o método de parse_news para extrair a notícia.
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

            self.logger.info(f"[{response.meta['date'].strftime('%d/%m')}] KW: {response.meta['keyword']} - qtd. URLs encontradas: {len(clean_links)}")

            for url in clean_links:
                if url in self.seen_urls:
                    # Se já está na memória, avisamos no terminal e pulamos
                    print(f"⏭️  Pulando URL [JÁ ESTÁ NO BANCO]: {url}")
                else:
                    news_meta = response.meta.copy()
                    news_meta.pop('playwright', None)
                    news_meta.pop('playwright_include_page', None)
                    news_meta.pop('playwright_page_methods', None)
                    
                    yield scrapy.Request(url, self.parse_news, meta=news_meta)
        finally:
            await page.close()


    # Método que, caso a requisição do navegador falhe (abrir a página), fecha a página para não sobrecarregar a memória RAM.
    async def errback_close(self, failure):
        if failure.request.meta.get("playwright_page"):
            await failure.request.meta["playwright_page"].close()
    
    
    # Método que chama dois métodos de parse (layout antigo e novo).
    def parse_news(self, response):
        if response.url in self.seen_urls: return

        title = response.css("h1.content-head__title::text").get() or response.css("h1.entry-title::text").get()
        if not title: return

        item = self.try_parse(response, self.parse_news_v1) or self.try_parse(response, self.parse_news_v2)

        if item:
            self.seen_urls.add(response.url)
            yield item
    
    
    # Método para selecionar qual versão de parse (v1 ou v2) será utilizada.
    def try_parse(self, response, method):
        try: return method(response)
        except: return None
    
    
    # Método para extrair a data de publicação e formatá-la.
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
    
    
    # Método que insere preenche os atributos da notícia de acordo com o que foi extraído pelos seletores CSS.
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
    
    
    # Método para formatar o corpo-texto da notícia -> Facilitar a leitura da classificação manual.
    def clean_text(self, text_list):
        if not text_list:
            return None
            
        # Junta, remove quebras de linha excessivas e espaços nas pontas
        full_text = ' '.join([t.strip() for t in text_list if t.strip()])
        return full_text if len(full_text) > 50 else None # Filtra textos muito curtos (provavelmente erro)
    
    
    # Método que usa seletores CSS para o layout antigo do G1 (páginas mais antigas).
    def parse_news_v1(self, response):
        sub = response.css('h2::text').getall()
        sub = ' '.join([s.strip() for s in sub if s.strip()])
        
        # Seletores para o corpo
        body_selectors = [
            "div#materia-letra p::text",
            "div.entry-content p::text",
            "div.post-content p::text"
        ]
        
        art = None
        for selector in body_selectors:
            texts = response.css(selector).getall()
            art = self.clean_text(texts)
            if art: break
            
        if not art: return None # Falha no parse V1

        full_art = (sub + ' ' + art).strip()
        dt = self.extract_date(response)
        return self._base_item(response, full_art, dt)
    
    
    # Método que usa seletores CSS para o layout moderno do G1.
    def parse_news_v2(self, response):
        sub = response.css("h2.content-head__subtitle::text").get() or \
              response.css("h2[itemprop='alternativeHeadline']::text").get() or ""
        
        texts = response.css("article p.content-text__container::text").getall()

        if not texts:
            texts = response.css("div.mc-column.content-text p::text").getall()

        if not texts:
            texts = response.css("article[itemprop='articleBody'] p::text").getall()

        if not texts:
            texts = response.css("div.widget--info__text-container p::text").getall()

        art = self.clean_text(texts)
        
        if not art: return None 

        full_art = (sub.strip() + " " + art).strip()
        dt = self.extract_date(response)
        
        return self._base_item(response, full_art, dt)


    # Método que preenche a lista de gangues ['gangs']
    def search_gangs(self, art):
        if not art: return []
        found = []
        for p in VALIDATION_KEYWORDS['GANGS']:
            found += re.findall(p, unidecode(art), re.IGNORECASE)
        return found
    
    
    # Método que preenche o atributo 'accepted_by'.
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
