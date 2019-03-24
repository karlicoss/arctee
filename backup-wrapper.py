#!/usr/bin/env python3.6
import argparse
from datetime import datetime
import logging
import time
import os.path
from pathlib import Path

import subprocess
from subprocess import Popen, PIPE, check_call

from typing import Optional

from tempfile import TemporaryDirectory

from atomicwrites import atomic_write
from kython.ktyping import PathIsh

# todo pip atomicwrites
# from atomicwrites import atomic_write
# TODO can't use at the moment since it doesn't seem to support binary writing :(

def get_logger():
    return logging.getLogger('backup-wrapper')


DATEFMT_FULL = "%Y%m%d%H%M%S"

Compression = Optional[str]


def apack(data: bytes, compression: Compression) -> bytes:
    if compression is None:
        return data
    with TemporaryDirectory() as td:
        res = Path(td).joinpath('result.' + compression)
        p = Popen([
            'apack', '-F', compression, str(res),
        ], stdin=PIPE)
        _, _ = p.communicate(input=data)
        assert p.returncode == 0

        return res.read_bytes()


def backup(dir_: PathIsh, prefix: str, command: str, datefmt: str, backoff: int, compression: Compression=None):
    bdir = Path(dir_)

    logger = get_logger()

    pname, ext = prefix.split('.')  # TODO meh

    if compression is not None:
        ext += '.' + compression

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
            if att != backoff - 1:
                logger.info('sleeping for a bit before retrying...')
                time.sleep(60 * 3)
        else:
            logger.info(errmsg)
            break
    if error is not None:
        raise RuntimeError(error)
    assert stdout is not None

    stdout = apack(data=stdout, compression=compression)

    unow = datetime.utcnow()
    dates = unow.strftime(datefmt)
    path = bdir.joinpath(pname + "_" + dates + "." + ext)
    logger.debug("Writing to " + path.as_posix())
    # TODO use renaming instead? might be easier...
    with atomic_write(path.as_posix(), mode='wb', overwrite=True) as fo:
        fo.write(stdout)


def test(tmp_path):
    tdir = Path(tmp_path)
    bdir = tdir.joinpath('backup')
    bdir.mkdir()

    def run(**kwargs):
        backup(
            bdir,
            prefix='testing.txt',
            command='printf "{}"'.format('0' * 1000),
            datefmt=DATEFMT_FULL,
            # TODO test backoff?
            backoff=1,
            **kwargs,
        )

    run(compression=None)
    [ff] = list(bdir.glob('*.txt'))
    assert ff.stat().st_size == 1000

    run(compression='xz')
    [xz] = list(bdir.glob('*.xz'))
    assert xz.stat().st_size == 76


def main():
    from kython.klogging import setup_logzero
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
    parser.add_argument(
        '--compression',
        default=None,
    )
    args = parser.parse_args()
    if not os.path.lexists(args.dir):
        raise RuntimeError(f"Directory {args.dir} doesn't exist!")

    datefmt = DATEFMT_FULL if args.new else "%Y-%m-%d"
    backup(args.dir, args.prefix, args.command, datefmt, backoff=args.backoff, compression=args.compression)

if __name__ == '__main__':
    main()

