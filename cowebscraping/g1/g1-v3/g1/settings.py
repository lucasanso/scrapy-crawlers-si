# settings.py

BOT_NAME = 'g1'

SPIDER_MODULES = ['g1.spiders']
NEWSPIDER_MODULE = 'g1.spiders'

# --- CONFIGURAÇÕES DO PLAYWRIGHT ---
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# É obrigatório usar o AsyncioSelectorReactor para o Playwright funcionar
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,  # True para não abrir a janela do navegador (mais rápido). False para ver acontecendo.
    "timeout": 20 * 1000,  # 20 segundos para abrir o browser
}

# Tempo máximo para carregar uma página no Playwright
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30 * 1000 

# --- OUTRAS CONFIGURAÇÕES ---

# Identificação de User-Agent real para evitar bloqueios simples
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Obey robots.txt rules?
# Como você mencionou que o robots.txt está bloqueando a busca, definimos como False
ROBOTSTXT_OBEY = False

# Concorrência: Playwright consome muita RAM. Mantenha baixo (4 a 8).
CONCURRENT_REQUESTS = 4

# Log level
LOG_LEVEL = 'INFO'

# Configurações padrão do Scrapy
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
# FEED_EXPORT_ENCODING = "utf-8"
