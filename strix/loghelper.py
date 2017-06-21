import logging
import logging.handlers
import os
from os import path
import sys
import shutil
from datetime import datetime
from datetime import timedelta
from strix.config import config


FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
os.makedirs("logs", exist_ok=True)


class MsgCounterHandler(logging.Handler):
    levelcount = None

    def __init__(self, *args, **kwargs):
        super(MsgCounterHandler, self).__init__(*args, **kwargs)
        self.levelcount = {}

    def emit(self, record):
        l = record.levelname
        self.levelcount.setdefault(l, 0)
        self.levelcount[l] += 1

logcounter = MsgCounterHandler()


def get_log_level():
    if config.has_attr("log_level"):
        if config.log_level == "DEBUG":
            return logging.DEBUG
        if config.log_level == "INFO":
            return logging.INFO
        if config.log_level == "WARN":
            return logging.WARN
        if config.log_level == "ERROR":
            return logging.ERROR

    return logging.INFO


def setup_pipeline_logging(task_name):
    formatter = logging.Formatter(FORMAT)
    log_level = get_log_level()

    current_logs = os.listdir("logs")
    if len(current_logs) > 0:
        os.makedirs("logs/old", exist_ok=True)
        for log in current_logs:
            shutil.move(path.join("logs", log), "logs/old")

    date_format = "%Y-%m-%d_%H:%M:%S"
    if path.exists("logs/old"):
        for old_log in os.listdir("logs/old"):
            if "__" in path.basename(old_log):
                date_str = old_log.split("__")[1]
                date = datetime.strptime(date_str, date_format)
                if date < (datetime.now() - timedelta(30)):
                    os.remove(path.join("logs/old", old_log))

    file_name = task_name + "__" + datetime.now().strftime(date_format)

    fh = logging.FileHandler("logs/pipeline-" + file_name, mode="w", encoding="UTF-8", delay=True)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)

    errh = logging.FileHandler("logs/pipeline-err-" + file_name, mode="w", encoding="UTF-8", delay=True)
    errh.setLevel(logging.ERROR)

    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setFormatter(formatter)

    logging.root.addHandler(ch)
    logging.root.addHandler(fh)
    logging.root.addHandler(errh)
    logging.root.addHandler(logcounter)
    logging.root.setLevel(log_level)


def setup_file_logging():
    log_level = get_log_level()
    fh = logging.handlers.RotatingFileHandler("logs/web.log", mode="a", encoding="UTF-8", maxBytes=5000000, backupCount=5)
    fh.setLevel(log_level)
    fh.setFormatter(logging.Formatter(FORMAT))
    logging.root.setLevel(log_level)
    logging.root.addHandler(fh)
