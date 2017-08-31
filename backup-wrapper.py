#!/usr/bin/env python3.6
import argparse
import datetime
import logging
import os.path
from pathlib import PosixPath
import subprocess
from subprocess import Popen

# todo pip atomicwrites
# from atomicwrites import atomic_write
# TODO can't use at the moment since it doesn't seem to support binary writing :(
# command just pipes output
# TODO coloredlogs?

logger = logging.getLogger('backup-wrapper')


def backup(dir_: str, prefix: str, command: str):
    pname, ext = prefix.split('.')  # TODO meh

    logger.debug(f"Running {command}")
    p = Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # TODO make sure err gets printed out
    out, err = p.communicate()

    logger.info(f"Stderr: {err.decode('ascii')}")

    if p.returncode != 0:
        raise RuntimeError(f"Return code of the command was {p.returncode}")

    today = datetime.date.today()
    path = PosixPath(dir_, pname + "_" + str(today) + "." + ext)
    logger.debug("Writing to " + path.as_posix())
    with path.open('wb') as fo:
        fo.write(out)


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '--dir',
        default=None, required=True,
        type=str,
        help="Directory to store backup"
       )
    parser.add_argument(
        '--prefix',
        default=None, required=True,
        type=str,
        help="Prefix to be prepended"
       )
    parser.add_argument(
        '--command',
        default=None, required=True,
        type=str,
        help="Command to be executed which outputs the data to back up"
       )
    args = parser.parse_args()
    # TODO check for dir existence
    if not os.path.exists(args.dir):
        raise RuntimeError("Directory {} doesn't exist!".format(args.dir))
    backup(args.dir, args.prefix, args.command)


def setup_logger():
    try:
        import coloredlogs
        coloredlogs.install(level=logging.DEBUG)
    except ImportError as e:
        logger.warning("coloredlogs is unavailable!")


        # TODO kython setup logging?
setup_logger()
main()
