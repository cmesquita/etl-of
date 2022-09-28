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
Diretório contendo o arquivo de configuração usado para customizar o comportamento do ETL.<br>
config.json possui parâmetros utilizados pelas funções de transformação.

```
{
	"comar_order": ["ABC","XYZ"],
	"importacao": ["INT","CSA"]
}
```

### Monitoring:
Diretório contendo a classe de monitoramento do structured streaming.<br>
A classe MyListener contém exemplo de código que gera um arquivo json para ciclo de micro-batch realizado pela spark structured stream.<br>
MyListener implementa uma interface para uma classe abstrata que interage com o spark query manager.

```
my_listener = MyListener()
spark.streams.addListener(my_listener)
```
### Utils:
Diretório contendo as funções de transformação do ETL.<br>
No nosso exemplo: <br>
	**checkCondition**:-> função que válida se o doc_type  pertence a uma lista de valores (ordens do tipo comar).
	**substringCondition**:-> função que válida se o doc_type  pertence a uma lista de valores (ordens do tipo importanção).

### Notebooks:
Diretório contendo o notebook com as etapas de extração, transformação e carga do ETL.
<br>
## Modo de usar
O script de ETL está adaptado para ser utilizado com o recurso repos do databricks. <br>
Clique [aqui](https://docs.databricks.com/repos/index.html) para ter acesso a documentação que possui informações de como clonar um repositório no github a partir do databricks.