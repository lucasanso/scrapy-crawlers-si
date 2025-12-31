# Crawler G1 com Scrapy-Playwright

Este é um programa escrito em Python e desenvolvido utilizando o framework Scrapy

Seu principal objetivo é obter notícias do portal G1.

## Instalação

(Preferencialmente em um ambiente virtual `$ virtualenv .venv`)

1. Geração do clone deste repositório com o comando:
    ```bash
    git clone https://gitlab.com/ivato/textanalysis/crimeorganizado/cowebscraping.git
    ```

2. Acesso ao projeto `G1-v3` especificamente:
    ```bash
    cd .\cowebscraping\web_scraping\g1-v3\
    ```

3. Criação e inicialização do ambiente virtual:
    ```
    virtualenv .venv
    source .venv/bin/activate
    ```

4. Instalação dos pacotes necessários:
    ```bash
    $ pip install -r requirements.txt
    ```

5. Especificação das credenciais de acesso ao banco de dados em um arquivo intitulado `config.yaml` e no seguinte formato:
    ```yml
    lamcad:
        server_ip: "<value>"
        server_port: <value>
        ssh_username: "<value>"
        ssh_password: "<value>"
        local_bind_ip: "<value>"
        local_bind_port: <value>
        remote_bind_ip: "<value>"
        remote_bind_port: <value>

    mongodb_lamcad:
        uri: "<value>"
        database: "<value>"
        accepted_news_collection: "<value>"
        unaccepted_news_collection: "<value>"
    ```

## Execução

### Modo geral
Basta executar o comando abaixo para iniciar a extração de notícias do G1 utilizando as palvras-chave do script `g1/keywords.py`:

```bash
$ scrapy crawl scrape
```
