#!/usr/bin/env python3.6
import argparse
from datetime import datetime
import logging
import os.path
from pathlib import PosixPath
import subprocess
from subprocess import Popen

from kython import atomic_write

# todo pip atomicwrites
# from atomicwrites import atomic_write
# TODO can't use at the moment since it doesn't seem to support binary writing :(

def get_logger():
    return logging.getLogger('backup-wrapper')


def backup(dir_: str, prefix: str, command: str, datefmt: str, backoff: int):
    logger = get_logger()

    pname, ext = prefix.split('.')  # TODO meh

    stdout = None
    stderr = None
    error = None
    for att in range(backoff):
        logger.debug(f"Running {command}, attempt {att + 1}/{backoff}")
        p = Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()

        errmsg = f"Stderr: {stderr.decode('utf-8')}"
        if p.returncode != 0:
            logger.error(errmsg)
            error = f"Non-zero return code: {p.returncode}"
        else:
            logger.info(errmsg)
            break
    if error is not None:
        raise RuntimeError(error)

    unow = datetime.utcnow()
    dates = unow.strftime(datefmt)
    path = PosixPath(dir_, pname + "_" + dates + "." + ext)
    logger.debug("Writing to " + path.as_posix())
    with atomic_write(path.as_posix(), 'wb') as fo:
        fo.write(stdout)


def main():
    from kython.logging import setup_logzero
    setup_logzero(get_logger(), level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='Generic backup tool')
    parser.add_argument(
        '--dir',
        help="Directory to store backup",
        type=str,
        default=None, required=True,
    )
    parser.add_argument(
        '--prefix',
        help="Prefix to be prepended",
        type=str,
        default=None, required=True,
    )
    parser.add_argument(
        '--command',
        help="Command to be executed which outputs the data to back up",
        type=str,
        default=None, required=True,
    )
    parser.add_argument(
        '--new',
        help="New timestamp format",
        action='store_true',
        default=False, required=False,
    )
    parser.add_argument(
        '--backoff',
        type=int,
        default=1,
    )
    args = parser.parse_args()
    if not os.path.lexists(args.dir):
        raise RuntimeError(f"Directory {args.dir} doesn't exist!")

    datefmt = "%Y%m%d%H%M%S" if args.new else "%Y-%m-%d"
    backup(args.dir, args.prefix, args.command, datefmt, backoff=args.backoff)

if __name__ == '__main__':
    main()

