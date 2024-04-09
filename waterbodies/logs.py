import logging
import sys


def logging_setup(verbose: int = 1):
    """
    Setup logging to print to stdout with default logging level being INFO.
    """

    if verbose == 0:
        level = logging.WARNING
    elif verbose == 1:
        level = logging.INFO
    elif verbose == 2:
        level = logging.DEBUG
    else:
        raise ValueError("Maximum verbosity is -vv (verbose=2)")

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
