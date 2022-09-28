@udf
def checkCondition(param1, param2):
    if param1 in ['ABC','XYZ']:
        return param2
    
@udf
def substringCondition(param1, param2):
    if param1 in ['INT','CSA']:
        return int((str(param2)[-2:]))
    else:
        return param2