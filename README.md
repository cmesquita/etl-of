# ETL tabelas order fulfillment John Deere
<br>
Esse repositório contém as etapas de extração, transformação e carga dos dados do ciclo de vida do processo de order fulfillment na john deere.
<br>
O repositório possui a seguinte estrutura de arquivos.
```
configs
   |-- config.json
   |-- config.py
monitoring
   |-- listener.py
notebooks
   |-- etl.py
utils
   |-- transformation.py
```

### Config:
Diretório contendo o arquivo de configuração usado para customizar o comportamento do ETL.
### Monitoring:
Diretório contendo a classe de monitoramento do structured streaming.
### Notebooks:
Diretório contendo o notebook com as etapas de extração, transformação e carga do ETL.
### Utils:
Diretório contendo as funções de transformação do ETL.
<br>
## Modo de usar
O script de ETL está adaptado para ser utilizado com o recurso repos do databricks. 
Clique [aqui](https://docs.databricks.com/repos/index.html) para ter acesso a documentação que possui informações de como clonar um repositório no gitgub a partir do databricks.