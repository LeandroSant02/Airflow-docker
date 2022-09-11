# Airflow-docker

# Extraçao de dados do Twitter usando Apache Airflow no Docker

Projeto com intuito de criar um datapipelina, usando a API do twitter.


## Pre-Requisitos

**Passo  - 1**

Instalar o Docker de seu site oficial: [site]([https://docs.docker.com/get-docker/](https://www.docker.com/products/docker-desktop/).
Obs: Meu desenvlvimento foi usando o windows.

**Step - 2**

Fazer uma conta no Twitter Developer  [(Apply for access - Twitter Developers | Twitter Developer)](https://developer.twitter.com/en/apply-for-access)

Após criar sua conta no Twitter Developer, obtenha as credenciais necessarias para o desenvolvimento, sao elas:

```
consumer_key = ''           
consumer_secret = ''        
access_token = ''           
access_token_secret = ''    
```

## Como funcionará o projeto? 

Executaremos o Airflow junto a um container e atraves dele faremos todo o desenvolvimento usando os seguintes Scripts:
docker-compose.yaml
twitter_search.py
transformation.py
