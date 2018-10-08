#!/usr/bin/env python3.6
import argparse
from datetime import datetime
import logging
import os.path
from pathlib import PosixPath
import subprocess
from subprocess import Popen

# todo pip atomicwrites
# from atomicwrites import atomic_write
# TODO can't use at the moment since it doesn't seem to support binary writing :(

logger = logging.getLogger('backup-wrapper')


def backup(dir_: str, prefix: str, command: str, datefmt: str):
    from kython import atomic_write

    pname, ext = prefix.split('.')  # TODO meh

    logger.debug(f"Running {command}")
    p = Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()


    errmsg = f"Stderr: {err.decode('utf-8')}"
    if p.returncode != 0:
        logger.error(errmsg)
        raise RuntimeError(f"Non-zero return code: {p.returncode}")
    else:
        logger.info(errmsg)


    unow = datetime.utcnow()
    dates = unow.strftime(datefmt)
    path = PosixPath(dir_, pname + "_" + dates + "." + ext)
    logger.debug("Writing to " + path.as_posix())
    with atomic_write(path.as_posix(), 'wb') as fo:
        fo.write(out)


def main():
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
    args = parser.parse_args()
    if not os.path.lexists(args.dir):
        raise RuntimeError(f"Directory {args.dir} doesn't exist!")

    datefmt = "%Y%m%d%H%M%S" if args.new else "%Y-%m-%d"
    backup(args.dir, args.prefix, args.command, datefmt)

if __name__ == '__main__':
    from kython.logging import setup_logzero
    setup_logzero(logger, level=logging.DEBUG)
    main()

