"""
    creating a constructor and instance for
    logger we can call the log4j 

    """


class Log4j:


    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "guru.learningjournal.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class+ "." + app_name)
   
    def warn(self, message):
        "pass on the message to logger warn "
        self.logger.warn(message)

    
    def info(self, message):
        "pass on the message to logger info "
        self.logger.info(message)

    
    def error(self, message):
        "pass on the message to logger error "
        self.logger.error(message)

    
    def debug(self, message):
        "pass on the message to logger debug"
        self.logger.debug(message)