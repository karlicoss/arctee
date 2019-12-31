#!/usr/bin/env python3
import argparse
from pathlib import Path
import logging
from subprocess import Popen, PIPE, run, CalledProcessError, check_output
from typing import Sequence, Optional


# todo pip atomicwrites
from atomicwrites import atomic_write


def get_logger():
    return logging.getLogger('exportwrapper')


def setup_logging():
    # TODO FIXME remove this
    from kython.klogging import setup_logzero
    setup_logzero(get_logger(), level=logging.DEBUG)
    setup_logzero(logging.getLogger('backoff'), level=logging.DEBUG)


_ISO_FORMAT = '%Y%m%dT%H%M%SZ'


def utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime(_ISO_FORMAT)


def hostname() -> str:
    import socket
    return socket.gethostname()


# don't think anything standard for that exists?
# https://github.com/borgbackup/borg/blob/d02356e9c06f980b3d53459c6cc9c264d23d499e/src/borg/helpers/parseformat.py#L205
PLACEHOLDERS = {
    'utcnow'  : utcnow,
    'hostname': hostname,
}


def replace_placeholders(s) -> str:
    pdict = {
        k: v() for k, v in PLACEHOLDERS.items()
    }
    return s.format(**pdict)


Compression = Optional[str]


def apack(data: bytes, compression: Compression) -> bytes:
    if compression is None:
        return data
    else:
        return check_output([
            'apack',
            '-F', compression,
            '-f', '/dev/stdout',  # -f flag to convince to 'overwrite' stdout
        ], input=data)


def do_command(command: str) -> bytes:
    logger = get_logger()
    logger.debug(f"Running '{command}'")

    r = run(command, shell=True, stdout=PIPE, stderr=PIPE)

    errmsg = f"Stderr: {r.stderr.decode('utf8')}"
    if r.returncode != 0:
        logger.error(errmsg)
        logger.error(f"Stdout: {r.stdout.decode('utf8')}")
        error = f"Non-zero return code: {r.returncode}"
        logger.error(error)
        r.check_returncode()
        raise AssertionError("shouldn't happen")

    logger.info(errmsg)
    return r.stdout


def get_stdout(*, retries: int, compression: Compression, command: str):
    import backoff # type: ignore
    retrier = backoff.on_exception(backoff.expo, exception=CalledProcessError, max_tries=retries)
    stdout = retrier(lambda: do_command(command))()
    stdout = apack(data=stdout, compression=compression)
    return stdout


def do_export(
        *,
        path: str,
        retries: int,
        compression: Compression,
        command: Sequence[str],
) -> None:
    logger = get_logger()
    assert len(command) > 0
    commands = ' '.join(command)  # deliberate shell-like behaviour

    stdout = get_stdout(command=commands, retries=retries, compression=compression)

    path = replace_placeholders(path)

    logger.debug("writing to %s", path)

    with atomic_write(path, mode='wb', overwrite=True) as fo:
        fo.write(stdout)


def main():
    setup_logging()

    p = argparse.ArgumentParser(
        description='Wrapper for automating routine for reliable and regular data exports', # TODO link?
        formatter_class=argparse.RawTextHelpFormatter,
    )
    pss = ', '.join("{" + p +  "}" for p in PLACEHOLDERS)
    p.add_argument('--path', type=str, required=True, help=f"""
Path with borg-style placeholders. Supported: {pss}.

Example: --path '/exports/pocket/pocket_{{utcnow}}.json'

(see https://manpages.debian.org/testing/borgbackup/borg-placeholders.1.en.html)
""".strip())
    # TODO add argument to treat path as is?
    p.add_argument(
        '--retries',
        help='Number of retries with exponential backoff before failing',
        type=int,
        default=1,
    )
    # TODO eh, ignore it?
    p.add_argument(
        '-c', '--compression',
        help='''
Set compression format (passed to 'apack -F').

See man apack for list of supported formats.
'''.strip(),
        default=None,
    )
    p.add_argument('command', nargs=argparse.REMAINDER, help='Rest of the arguments are the actual command to run')

    args = p.parse_args()

    path = args.path
    # we want to timestamp _after_ we ran the command
    # s othis is early check for pattern validity, before running the command
    replace_placeholders(path)

    command = args.command
    # https://stackoverflow.com/questions/25872515/python-argparse-treat-arguments-in-different-ways#comment52606932_25873028
    if command[0] == '--':
        del command[0]

    do_export(path=path, retries=args.retries, compression=args.compression, command=command)


if __name__ == '__main__':
    main()


def test(tmp_path):
    tdir = Path(tmp_path)
    bdir = tdir.joinpath('backup')
    bdir.mkdir()

    def run(**kwargs):
        do_export(
            path=str(bdir / 'test_{utcnow}_{hostname}.txt'),
            retries=1,
            # TODO test backoff?
            command=['printf', '0' * 1000],
            **kwargs,
        )

    run(compression=None)
    [ff] = list(bdir.glob('*.txt'))
    assert ff.stat().st_size == 1000

    run(compression='xz')
    # note that extension has to be in sync with --compression argument at the moment...
    # wish apack had some sort of 'noop' mode...
    [xz] = list(bdir.glob('*.txt'))
    assert xz.stat().st_size == 76

