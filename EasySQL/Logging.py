import logging
import colorlog


__all__ = [
    "logger", "enable_debug", "disable_debug",
    "debug", "info", "warning", "warn", "error", "critical"
]


class LevelDependentFormatter(colorlog.ColoredFormatter):
    def __init__(self, base_fmt, debug_fmt, *args, **kwargs):
        super().__init__(base_fmt, *args, **kwargs)
        self.base_fmt = base_fmt
        self.debug_fmt = debug_fmt

    def format(self, record):
        if record.levelno == logging.DEBUG: self._style._fmt = self.debug_fmt
        else: self._style._fmt = self.base_fmt
        return super().format(record)


def get(name: str = None, colors: dict = None):
    instance_logger = logging.getLogger(name)
    if getattr(instance_logger, "_is_formatted", False):
        return instance_logger

    if colors is None:
        colors = {"DEBUG": "white", "INFO": "light_white", "WARNING": "yellow", "ERROR": "red", "CRITICAL": "white,bg_red"}

    base_fmt = "%(log_color)s[%(levelname)s" + ("]" if instance_logger == logging.root else " - %(name)s]") + " %(asctime)s: %(message)s"
    debug_fmt = base_fmt.replace("]"," @%(filename)s:%(lineno)d]")

    formatter = LevelDependentFormatter(base_fmt=base_fmt, debug_fmt=debug_fmt, log_colors=colors, reset=True, secondary_log_colors={}, style='%')

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)

    if instance_logger.handlers:
        instance_logger.handlers.clear()

    instance_logger.addHandler(handler)
    instance_logger.propagate = False
    setattr(instance_logger, "_is_formatted", True)
    return instance_logger


logger = logging.getLogger('EasySQL')
debug = logger.debug
info = logger.info
warning = logger.warning
warn = warning
error = logger.error
critical = logger.critical


def enable_debug():
    logger.setLevel(logging.DEBUG)


def disable_debug():
    logger.setLevel(logging.INFO)