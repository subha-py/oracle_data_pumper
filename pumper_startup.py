#!/u02/oracle_data_pumper/venv/bin/python
from utils.log import set_logger
# /etc/systemd/system/pumper-startup.service uses this file in pumper
def startup_activities(logger):
    logger.info('hello world!')

if __name__ == '__main__':
    logger=set_logger('pumper_startup_logger.log')
    startup_activities(logger)