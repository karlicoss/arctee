#!/usr/bin/env python3
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

import backoff # type: ignore

# hacky way to make backoff retries dynamic...
class RetryMe(Exception):
    pass


def backoff_n_times(f, attempts: int):
    @backoff.on_exception(backoff.expo, RetryMe, max_tries=attempts)
    def _do():
        return f()
    return _do


def do_command(command: str):
    logger = get_logger()
    logger.debug(f"Running {command}")
    p = Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    errmsg = f"Stderr: {stderr.decode('utf8')}"
    if p.returncode != 0:
        logger.error(errmsg)
        logger.error(f"Stdout: {stdout.decode('utf8')}")
        error = f"Non-zero return code: {p.returncode}"
        raise RetryMe

    logger.info(errmsg)
    return stdout

def get_stdout(command: str, backoff: int, compression: Compression=None):
    stdout = backoff_n_times(lambda: do_command(command), attempts=backoff)()
    stdout = apack(data=stdout, compression=compression)
    return stdout


def backup(dir_: PathIsh, prefix: str, command: str, datefmt: str, backoff: int, compression: Compression=None):
    bdir = Path(dir_)

    logger = get_logger()

    pname, ext = prefix.split('.')  # TODO meh

    if compression is not None:
        ext += '.' + compression

    stdout = get_stdout(command, backoff, compression)

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


def setup_parser(parser):
    parser.add_argument(
        '--backoff',
        type=int,
        default=1,
    )
    parser.add_argument(
        '--compression',
        default=None,
    )


def setup_logging():
    from kython.klogging import setup_logzero
    setup_logzero(get_logger(), level=logging.DEBUG)
    setup_logzero(logging.getLogger('backoff'), level=logging.DEBUG)


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

    setup_parser(parser)
    datefmt = DATEFMT_FULL if args.new else "%Y-%m-%d"
    backup(args.dir, args.prefix, args.command, datefmt, backoff=args.backoff, compression=args.compression)

if __name__ == '__main__':
    main()

