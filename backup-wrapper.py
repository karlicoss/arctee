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

# todo pip atomicwrites

def get_logger():
    return logging.getLogger('backup-wrapper')


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


def test(tmp_path):
    tdir = Path(tmp_path)
    bdir = tdir.joinpath('backup')
    bdir.mkdir()

    def run(**kwargs):
        # TODO fix the test
        # pylint: disable=undefined-variable
        backup( # type: ignore
            bdir,
            prefix='testing.txt',
            command='printf "{}"'.format('0' * 1000),
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
