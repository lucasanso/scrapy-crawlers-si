from unidecode import unidecode
import re

from .keywords import VALIDATION_KEYWORDS

def accept_article(article):
    validation_keywords = sum(VALIDATION_KEYWORDS.values(), start=[])
    for pattern in validation_keywords:
        if re.findall(pattern, unidecode(article.lower()), re.IGNORECASE):
            return pattern
    return False

def search_gangs(article):
    gangs_found = []
    for pattern in VALIDATION_KEYWORDS['GANGS']:
        gangs_found += re.findall(pattern, unidecode(article), re.IGNORECASE)
    return gangs_found