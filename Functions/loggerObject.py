import logging

"""
Desc:
    Creates a logging with set log file and name.

Arg:
    :loggerName: Name of Logger.
    :fileName: File name for log data to be written into.
""" 
def createLogger(loggerName:str, fileName:str) -> logging:
    
    logger = logging.getLogger(loggerName)
    f_handler = logging.FileHandler(fileName)
    #f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    logger.setLevel(logging.INFO)
    
    return logger