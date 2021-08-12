import logging
import os

class TopOrderBookWriter():
    def __init__(self, product):
        #today    = dt.datetime.now()
        filename = 'TopOrderbook_'+ product + "_"+ today.strftime("%Y%m%d%H%M%S") + '.log'
        log_file = os.getcwd() + filename
        logger   = logging.getLogger(__name__)
        logging.basicConfig(filename=log_file, level=logging.INFO, filemode= 'a',
                            format='%(asctime)s.%(msecs)03d %(message)s')
        logger.info("bidprice, bidsize, askprice, asksize")
        self.logger =  logger


