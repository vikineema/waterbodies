import logging
import sys


def logging_setup(verbose: int = 1):
    """
    Setup logging to print to stdout with default logging level being CRITICAL.
    """
    if verbose == 1:
        level = logging.CRITICAL
    elif verbose == 2:
        level = logging.ERROR
    elif verbose == 3:
        level = logging.INFO
    elif verbose == 4:
        level = logging.DEBUG
    else:
        raise ValueError("Maximum verbosity is -vvvv (verbose=4)")

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
