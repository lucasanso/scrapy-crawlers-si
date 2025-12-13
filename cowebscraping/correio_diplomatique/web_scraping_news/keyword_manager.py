from .keywords import SEARCH_KEYWORDS, VALIDATION_KEYWORDS
import re
from unidecode import unidecode

class KeywordManager:
    def __init__(self, keyword=None, continue_scraping=False, stop_keywords=None):
        self.search_keywords = SEARCH_KEYWORDS
        self.validation_keywords = VALIDATION_KEYWORDS
        self.user_keyword = keyword
        self.continue_scraping = continue_scraping
        self.stop_keywords = stop_keywords

    def get_search_keywords(self):
        if self.continue_scraping and self.stop_keywords:
            try:
                # Encontrar o índice da palavra-chave
                index = self.search_keywords.index(self.stop_keywords)
                # Retornar as palavras que vêm após a palavra-chave fornecida
                return self.search_keywords[index :]
            except ValueError:
                # Caso a palavra não esteja na lista, retornar a lista original
                print(f"Palavra-chave '{self.stop_keywords}' não encontrada.")
                return self.search_keywords
        else:
            if self.user_keyword is None:
                return self.search_keywords
            else :
                keywords = [keyword.strip() for keyword in self.user_keyword.split(',')]
                return keywords

    # --- MÉTODOS DE VALIDAÇÃO (LÓGICA DO G1) ---

    def organized_crime_keyword(self, article_text):
        """
        Verifica se o texto contém palavras dos grupos:
        - GANGS (ex: CV, PCC)
        - ORGANIZED CRIME (ex: milícia, facção)
        """
        # Agrupamento das tabelas 1 e 2
        # O uso de .get() evita erro caso a chave não exista no dicionário
        validation_keywords = self.validation_keywords.get('GANGS', []) + self.validation_keywords.get('ORGANIZED CRIME', [])
        
        for pattern in validation_keywords:
            # Percorre o texto tentando encontrar uma palavra do grupo
            if re.findall(pattern, unidecode(article_text.lower()), re.IGNORECASE):
                return pattern
        return False
    
    def action_keyword(self, article_text):
        """
        Verifica se o texto contém palavras dos grupos:
        - DRUGS (ex: maconha, cocaína)
        - ARMED INTERACTIONS (ex: tiroteio, apreensão)
        """
        # Agrupamento das tabelas 3 e 4
        validation_keywords = self.validation_keywords.get('DRUGS', []) + self.validation_keywords.get('ARMED INTERACTIONS', [])
        
        for pattern in validation_keywords:
            # Percorre o texto tentando encontrar uma palavra do grupo
            if re.findall(pattern, unidecode(article_text.lower()), re.IGNORECASE):
                return pattern
        return False

    def accept_article(self, item):
        """
        Regra de Ouro:
        Só aceita se tiver (Gangue OU Crime Organizado) E (Drogas OU Ação Armada).
        """
        # Garante que pegamos o texto, mesmo que venha vazio
        article_text = item.get('article', '')
        if not article_text:
            return False

        # Teste 1: Grupo Sujeito (Gangue)
        gang_check = self.organized_crime_keyword(article_text)
        
        # Teste 2: Grupo Ação (Drogas/Armas)
        action_check = self.action_keyword(article_text)

        # Validação Final
        if gang_check and action_check:
            # Retorna uma string identificando o que foi achado (igual ao G1)
            # Ex: "pcc - tráfico de drogas"
            return f"{gang_check} - {action_check}"
        
        return False

    def search_gangs(self, item):
        """
        Busca especificamente nomes de gangues para preencher o campo 'gangs'
        """
        article_text = item.get('article', '')
        gangs_found = []
        
        # Procura apenas na lista específica de GANGS
        for pattern in self.validation_keywords.get('GANGS', []):
            matches = re.findall(pattern, unidecode(article_text), re.IGNORECASE)
            gangs_found.extend(matches)
            
        return gangs_found # Retorna a lista (mesmo que vazia)
