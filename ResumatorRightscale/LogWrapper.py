import logging

class LogWrapper():

    def __init__(self, logpath, logname):
        logger          = logging.getLogger(logname)

        if not len(logger.handlers):
            logfile     = '/'.join([logpath, "%s.log" % logname])
            format      = "%(levelname)s: %(asctime)s: %(message)s"
            dateformat  = "%Y/%m/%d %I:%M:%S %p"
            logger.setLevel(logging.DEBUG)
            fh          = logging.FileHandler(logfile)
            sh          = logging.StreamHandler()
            formatter   = logging.Formatter(format, dateformat)
            
            fh.setLevel(logging.DEBUG)
            sh.setLevel(logging.INFO)
            fh.setFormatter(formatter)
            sh.setFormatter(formatter)
            logger.addHandler(fh)
            logger.addHandler(sh)

        self.logger = logger

    def get_logger(self):
        return self.logger