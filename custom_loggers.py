import logging


def create_logger(name, level, file_name):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    file_handler = logging.FileHandler(file_name, mode='w', encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


debug_logger = create_logger('debug', logging.DEBUG, 'logs/debug.log')
info_logger = create_logger('info', logging.INFO, 'logs/info.log')
warning_logger = create_logger('warning', logging.WARNING, 'logs/warning.log')
error_logger = create_logger('error', logging.ERROR, 'logs/error.log')
critical_logger = create_logger('critical', logging.CRITICAL, 'logs/critical.log')
