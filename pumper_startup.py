from utils.log import set_logger

def startup_activities(logger):
    logger.info('hello world!')

if __name__ == '__main__':
    logger=set_logger('pumper_startup_logger.log')
