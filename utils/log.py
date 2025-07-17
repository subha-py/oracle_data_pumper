import logging
import os
import sys


# Get the directory where the script is located
def set_logger(log_file_name, dir=None):
    os.environ.setdefault("log_file_name", log_file_name)
    script_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
    if dir is not None:
        script_dir = os.path.join(script_dir, dir)
    log_filename = log_file_name + '.log'
    log_filepath = os.path.join(script_dir, log_filename)
    if os.path.exists(log_filepath):
        os.remove(log_filepath)
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filepath, mode='a')
    file_handler.setLevel(logging.INFO)  # Level for this handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Level for this handler
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger