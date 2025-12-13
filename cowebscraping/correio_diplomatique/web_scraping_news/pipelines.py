import json
import sys
import yaml
import pymongo
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from sshtunnel import open_tunnel

# --- CARREGAMENTO DO ARQUIVO DE CONFIGURAÇÃO ---
try:
    with open('config.yaml', 'r') as configs_file:
        configs = yaml.safe_load(configs_file)
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo config.yaml não encontrado!")
    sys.exit(1)

# --- PIPELINE DE ARMAZENAMENTO ---
class StoragePipeline:
    def __init__(self, output_mode='json'):
        self.output_mode = output_mode
        self.approved_file = None
        self.rejected_file = None
        
        # Variáveis do MongoDB e SSH
        self.client = None
        self.db = None
        self.server = None 

    @classmethod
    def from_crawler(cls, crawler):
        output_mode = crawler.settings.get('OUTPUT_MODE', 'json')
        return cls(output_mode)

    def open_spider(self, spider):
        if self.output_mode == 'json':
            self.approved_file = open('approved_items.json', 'a', encoding='utf-8')
            self.rejected_file = open('rejected_items.json', 'a', encoding='utf-8')
            
        elif self.output_mode == 'database':
            # --- CONEXÃO SSH E MONGODB ---
            # O Middleware abriu e fechou o túnel para LER. O Pipeline abre e MANTÉM aberto para ESCREVER.
            lamcad_configs = configs['lamcad']
            mongo_configs = configs['mongodb_lamcad']

            try:
                # 1. Abre o Túnel SSH
                self.server = open_tunnel(
                    (lamcad_configs['server_ip'], lamcad_configs['server_port']),
                    ssh_username=lamcad_configs['ssh_username'],
                    ssh_password=lamcad_configs['ssh_password'],
                    local_bind_address=(lamcad_configs['local_bind_ip'], lamcad_configs['local_bind_port']),
                    remote_bind_address=(lamcad_configs['remote_bind_ip'], lamcad_configs['remote_bind_port'])
                )
                self.server.start()
                spider.logger.info(f"✅ Pipeline: Túnel SSH aberto na porta local: {self.server.local_bind_port}")

                # 2. Conecta ao MongoDB através do túnel
                self.client = pymongo.MongoClient(mongo_configs['uri'])
                self.db = self.client[mongo_configs['database']]
                
                spider.logger.info(f"✅ Pipeline: Conectado ao MongoDB: {mongo_configs['database']}")

            except Exception as e:
                spider.logger.error(f"❌ Pipeline: ERRO CRÍTICO ao conectar no banco ou SSH: {e}")

        else:
            raise ValueError(f"Modo de saída desconhecido: {self.output_mode}")

    def close_spider(self, spider):
        if self.output_mode == 'json':
            if self.approved_file: self.approved_file.close()
            if self.rejected_file: self.rejected_file.close()
                
        elif self.output_mode == 'database':
            if self.client:
                self.client.close()
            if self.server:
                self.server.stop()
                spider.logger.info("Pipeline: Túnel SSH fechado.")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if self.output_mode == 'database':
            if self.db is None:
                return item

            url = adapter.get('url')
            newspaper = getattr(spider, 'article_newspaper_selector', None)

            # --------------------------------------------------------------------------------------
            # ETAPA 1: Salvar na coleção visitedUrls (TODAS as notícias) - SEM VERIFICAR DUPLICIDADE
            # O Middleware já filtrou duplicatas de REQUESTS. Se chegou aqui, é uma URL nova.
            # --------------------------------------------------------------------------------------
            
            # Usamos update_one com upsert=True para ser idempotente e não ter problemas de concorrência
            # Caso a URL tenha sido adicionada por outro processo/thread após o filtro do Middleware.
            self.db['visitedUrls'].update_one(
                {'url': url}, 
                {'$set': {'url': url, 'newspaper': newspaper}},
                upsert=True # Insere se não existir
            )
            
            # ---------------------------------------------------------
            # ETAPA 2: Salvar na coleção newsData (Apenas ACEITAS)
            # ---------------------------------------------------------
            # ETAPA 2: Salvar na coleção newsData (Apenas ACEITAS)
            # ---------------------------------------------------------
            if adapter.get('accepted_by'):
                
                # Vamos tentar INSERIR. Se der erro de duplicata, capturamos o erro.
                try:
                    # Lógica de ID Incremental (Só faz sentido calcular se for inserir)
                    last_item = self.db['newsData'].find_one(
                        {}, 
                        sort=[("id_event", -1)] 
                    )
                    next_id_event = (last_item['id_event'] + 1) if last_item else 1
                    adapter['id_event'] = next_id_event
                    
                    # TENTA INSERIR
                    self.db['newsData'].insert_one(dict(adapter))
                    
                    # Se chegou aqui, é porque deu certo
                    print(f"✅ [DB] Notícia ACEITA salva em newsData (ID {next_id_event}): {url}")

                except pymongo.errors.DuplicateKeyError:
                    # Se cair aqui, é porque já existia. Apenas avisamos e seguimos a vida.
                    print(f"⚠️ [DB] Notícia já existe em newsData (Ignorada): {url}")
                    
                    
        elif self.output_mode == 'json':
            line = json.dumps(dict(adapter), ensure_ascii=False) + "\n"
            if adapter.get('accepted_by'):
                self.approved_file.write(line)
            else:
                self.rejected_file.write(line)

        return item
