import logging
import os
import sys



#: The directory that contains the log files created by this package
LOG_DIR = "PulsarpyDx"
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:   %(message)s')
chandler = logging.StreamHandler(sys.stdout)
chandler.setLevel(logging.DEBUG)
chandler.setFormatter(formatter)
logger.addHandler(chandler)

# Add debug file handler
fhandler = logging.FileHandler(filename=os.path.join(LOG_DIR,"log_debug_dx-seq-import.txt"),mode="a")
fhandler.setLevel(logging.DEBUG)
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)

# Add error file handler
err_h = logging.FileHandler(filename=os.path.join(LOG_DIR,"log_error_dx-seq-import.txt") ,mode="a")
err_h.setLevel(logging.ERROR)
err_h.setFormatter(formatter)
logger.addHandler(err_h)
