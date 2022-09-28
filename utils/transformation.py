from pyspark.sql.functions import udf
from configs.config import getConfig

comar_order = getConfig("comar_order")
importacao =  getConfig("importacao")

@udf
def checkCondition(param1, param2):
    if param1 in comar_order:
        return param2
    
@udf
def substringCondition(param1, param2):
    if param1 in importacao:
        return int((str(param2)[-2:]))
    else:
        return param2