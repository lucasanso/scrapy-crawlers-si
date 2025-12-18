from itemadapter import ItemAdapter
from scrapy.exporters import JsonItemExporter
from sshtunnel import open_tunnel
import pymongo
import sys
import yaml

from .items import G1Item

# Abre as credenciais do MongoDB que estão no arquivo config.yaml
try:
    with open('config.yaml', 'r') as configs_file:
        configs = yaml.safe_load(configs_file)
except FileNotFoundError:
    print("ERRO CRÍTICO: Arquivo config.yaml não encontrado!")
    
    # O programa terminou com erro (com o 0 seria sucesso).
    sys.exit(1)

# MongoDB LaMCAD
class MongoDBPipeline:
    def __init__(self):
        self.mongodb_uri = configs['mongodb_lamcad']['uri']
        self.mongodb_database = configs['mongodb_lamcad']['database']
        self.mongodb_accepted_news_collection = configs['mongodb_lamcad']['accepted_news_collection']
        self.mongodb_unaccepted_news_collection = configs['mongodb_lamcad']['unaccepted_news_collection']
        
        self.server = None
        self.client = None

    def open_spider(self, spider):
        # Fazendo a conexão ssh com o servidor
        lamcad_configs = configs['lamcad']
        try:
            self.server = open_tunnel(
                (lamcad_configs['server_ip'], lamcad_configs['server_port']),
                ssh_username=lamcad_configs['ssh_username'],
                ssh_password=lamcad_configs['ssh_password'],
                local_bind_address=(lamcad_configs['local_bind_ip'], lamcad_configs['local_bind_port']),
                remote_bind_address=(lamcad_configs['remote_bind_ip'], lamcad_configs['remote_bind_port'])
            )
            self.server.start()
            spider.logger.info(
                f"Conexão com o LamCAD criada com o seguinte IP e porta: {self.server.local_bind_address}")

            # Obtendo acesso ao banco de dados
            self.client = pymongo.MongoClient(self.mongodb_uri)
            database = self.client[self.mongodb_database]
            self.accepted_news_collection = database[self.mongodb_accepted_news_collection]
            self.unaccepted_news_collection = database[self.mongodb_unaccepted_news_collection]
        except Exception as e:
            spider.logger.error(f"Erro crítico ao conectar no banco ou SSH: {e}")

    def close_spider(self, spider):
        if self.client:
            self.client.close()
        if self.server:
            self.server.stop()
    
    def process_item(self, item, spider):
        # Transforma o item Scrapy em um dicionário Python
        data = dict(G1Item(item))
        
        # Verifica se foi aceito pela flag que definimos no scrape.py
        is_accepted = data.get('accepted_by')

        if is_accepted:
            self.set_news_data(data)
            print(f"✅ [MONGODB] Inserindo notícia ACEITA: {data.get('url')}")
            
            # Insere na coleção de aceitos com todos os dados
            self.accepted_news_collection.insert_one(data)
            
            # Remove da coleção de não aceitos se já estiver lá (para evitar duplicidade entre coleções)
            self.unaccepted_news_collection.delete_one({'url': data.get('url')})
            
        else:
            # --- CAMINHO 2: APENAS URL VISITADA (UNACCEPTED) ---
            # Verifica se a URL já existe na coleção para não duplicar
            if not self.unaccepted_news_collection.find_one({'url': data.get('url')}):
                print(f"🚫 [MONGODB] Salvando na coleção UNACCEPTED (Apenas URL): {data.get('url')}")
                
                # --- AQUI ESTÁ A MUDANÇA QUE VOCÊ PEDIU ---
                # Criamos um dicionário contendo APENAS a URL.
                # O MongoDB vai adicionar o _id automaticamente.
                minimal_data = {
                    'url': data.get('url')
                }
                
                self.unaccepted_news_collection.insert_one(minimal_data)
            else:
                # print(f"⏭️ URL já existe no Unaccepted (Pulando): {data.get('url')}")
                pass
                
        return item
    
    def get_accepted_news_count(self):
        return self.accepted_news_collection.count_documents({}) # sem filtro, ou seja, qualquer coisa escrita vai somar

    def get_next_id_event(self):
        # Função que calcula o próximo id da coleção de notícias aceitas. 
        last_record = self.accepted_news_collection.find_one(sort=[('id_event', -1)])
        
        if last_record and 'id_event' in last_record:
            return last_record['id_event'] + 1
            
        # é como se tivesse um else aqui, para caso o banco esteja vazio, daí retorna 1.
        return 1 

    def set_news_data(self, news):
        # Define todos os campos extras como None e insere o id obtido em get_next_id_event.
        
        news['manual_relevance_class'] = None
        news['automatic_relevance_class'] = None
        news['relevance_model'] = None
        news['certainty_level'] = None
        news['relevance_classification_date'] = None
        news['id_event'] = self.get_next_id_event()
        news['confidence_relevance_class'] = None
        news['resumo'] = None
        news['data_evento'] = None
        news['quant_mortes'] = None
        news['tipo_droga'] = None
        news['quant_droga'] = None
        news['pais'] = None
        news['regiao'] = None
        news['municipio'] = None
        news['coordenados_GPS'] = None
        news['tipo_conflito_armado'] = None
        news['ator1_nome'] = None
        news['ator1_cod'] = None
        news['ator2_nome'] = None
        news['ator2_cod'] = None
        news['tipo_relacao_entre_atores'] = None
