import scrapy
import re
from .base_spider import BaseSpider

class SpiderDiplomatique(BaseSpider):
    """
    Spider para o Le Monde Diplomatique
    """
    name = 'diplomatique_news'
    allowed_domains = ['diplomatique.org.br']

    search_url_template = 'https://diplomatique.org.br/page/1/?s={keyword}&orderby=date&order=DESC'
    search_results_selector = '//h3/a/@href | //h2/a/@href'
    next_page_selector = '//a[@class="number nextp"]/@href'
    
    article_title_selector = '//h1[contains(@class, "post-title")]/a/text()'
    article_date_selector = '//time[contains(@class, "entry-date")]/@datetime | //time[contains(@class, "datapublicacao")]/@datetime'
    article_author_selector = '//span[@class="author vcard"]/text()'
    article_content_selector = '//div[@class="entry-content"]/p/text()'
    article_newspaper_selector = 'Le Monde Brasil Diplomatique'

    # --- CORREÇÃO AQUI ---
    # Defina um XPath que não existe para evitar o crash, 
    # ou o XPath real se houver paywall.
    payed_articles_selector = '//div[@class="classe-que-nao-existe"]'

class SpiderCorreioDoPovo(BaseSpider):
    """
    Spider para o Correio do Povo (Site dinâmico, usa Playwright)
    Estratégia: Extração via JS e Paginação Manual
    """
    name = 'correio_do_povo_news'
    allowed_domains = ['correiodopovo.com.br']

    search_url_template = 'https://www.correiodopovo.com.br/busca?q={keyword}&page={page_number}&sort=date'
    
    # Seletor visual do botão "Próximo" (usado apenas para verificar se existe mais páginas)
    next_page_selector = '//li/a[@title="Next page"]' 
    
    # Seletores internos da notícia
    article_title_selector = '//h1[contains(@class, "article__headline")]/text() | //h1/text()'
    article_date_selector = '//time/@datetime'
    article_author_selector = '//div[contains(@class, "autoredata")]//address/text()'
    article_content_selector = '//div[contains(@class, "article__body")]/p//text() | //div[contains(@class, "content-text")]/p//text()'
    
    article_newspaper_selector = 'correio do povo'
    payed_articles_selector = '//div[contains(@class, "conteudo_pago")]'

    # Regex para validar se é uma notícia real (termina com padrão numérico de ID)
    # Exemplo: noticia-titulo-1.54897
    news_pattern = re.compile(r'-\d+\.\d+$') 

    def process_next_keyword(self):
        if self.keyword_index < len(self.search_keywords):
            self.current_keyword = self.search_keywords[self.keyword_index]
            self.keyword_index += 1
            
            # Começa sempre na página 1 para a nova palavra-chave
            search_url = self.search_url_template.format(
                keyword=self.current_keyword.replace(' ', '+'), 
                page_number=1
            )
            
            self.logger.info(f"Iniciando busca com a palavra-chave: {self.current_keyword}")
            self.outstanding_requests = 1
            
            yield scrapy.Request(
                url=search_url, 
                callback=self.parse_search_results,
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'errback': self.errback_close_page,
                }
            )
        else:
            self.logger.info("🏁 Todas as palavras-chave foram processadas.")

    async def parse_search_results(self, response):
        page = response.meta.get("playwright_page")
        try:
            self.logger.info(f"Processando página de busca: {response.url}")
            
            # 1. Rola a página para baixo (Trigger de Lazy Load)
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                # Espera curta para garantir carregamento de elementos
                await page.wait_for_timeout(3000) 
            except:
                pass

            # 2. Extração via JS "Bala de Canhão"
            # Pega TODOS os links da página diretamente do navegador
            hrefs = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('a')).map(a => a.href).filter(h => h);
            }""")

            self.logger.info(f"O navegador viu {len(hrefs)} links totais.")

            # 3. Filtragem Inteligente (Python)
            article_links = []
            for link in hrefs:
                # Remove lixo básico
                if not link or 'javascript:' in link or 'mailto:' in link or 'whatsapp:' in link:
                    continue
                
                # Garante URL absoluta
                if not link.startswith('http'):
                    link = "https://www.correiodopovo.com.br" + (link if link.startswith('/') else '/' + link)

                # FILTRO DE OURO: Verifica se tem o padrão de ID de notícia
                # Isso garante que só pegamos notícias e ignoramos menus/propagandas
                if self.news_pattern.search(link):
                    article_links.append(link)
            
            # Remove duplicatas
            article_links = list(set(article_links))

            if not article_links:
                self.logger.warning("Nenhum link de notícia encontrado nesta página.")
            else:
                self.logger.info(f"SUCESSO! {len(article_links)} notícias identificadas para extração.")

            # Dispara os requests para as notícias (Modo Estático = Mais Rápido)
            for full_link in article_links:
                self.outstanding_requests += 1
                yield scrapy.Request(
                    url=full_link, 
                    callback=self.parse_item,
                    errback=self.handle_failure, # <--- ADICIONE (Evita travar se der erro 404)
                    dont_filter=True             # <--- ADICIONE (Evita travar se for duplicada)
                )

            # -----------------------------------------------------------
            # 4. PAGINAÇÃO ROBUSTA (Cálculo Manual)
            # -----------------------------------------------------------
            
            # Descobre a página atual pela URL
            current_page_match = re.search(r'page=(\d+)', response.url)
            current_page = int(current_page_match.group(1)) if current_page_match else 1
            next_page_num = current_page + 1
            
            # Verifica visualmente se existe um botão "Próximo" ou ícone de seta
            # Isso evita que o robô tente a página 1000 se ela não existe
            content = await page.content()
            selector = scrapy.Selector(text=content)
            
            # Procura pelo botão next no HTML renderizado
            has_next_button = selector.xpath(self.next_page_selector).get()

            if has_next_button:
                next_page_url = self.search_url_template.format(
                    keyword=self.current_keyword.replace(' ', '+'), 
                    page_number=next_page_num
                )
                
                self.logger.info(f"Indo para a próxima página: {next_page_num}")
                self.outstanding_requests += 1
                
                yield scrapy.Request(
                    url=next_page_url, 
                    callback=self.parse_search_results,
                    meta={
                        'playwright': True,
                        'playwright_include_page': True,
                        'errback': self.errback_close_page,
                    }
                )
            else:
                self.logger.info("Fim da paginação (Botão 'Next' não encontrado).")

        except Exception as e:
            self.logger.error(f"Erro crítico no Playwright: {e}")
        
        finally:
            # Fecha a aba para liberar memória
            if page:
                try:
                    await page.close()
                except:
                    pass

        self.outstanding_requests -= 1
        if self.outstanding_requests == 0:
            # NÃO chame process_next_keyword diretamente.
            # Use check_and_advance para garantir que salva no YAML e reseta o contador.
            for req in self.check_and_advance():
                yield req

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            try:
                await page.close()
            except:
                pass
        self.logger.error(f"Falha na requisição Playwright: {failure}")
        self.outstanding_requests -= 1
        if self.outstanding_requests == 0:
             for req in self.process_next_keyword():
                yield req
