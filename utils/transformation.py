from pyspark.sql.functions import udf

@udf
def checkCondition(param1, param2, param3):
    if param1 in param3:
        return param2
    
@udf
def substringCondition(param1, param2, param3):
    if param1 in param3:
        return int((str(param2)[-2:]))
    else:
        return param2