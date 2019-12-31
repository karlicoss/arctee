#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Sequence, Optional


# todo pip atomicwrites
from atomicwrites import atomic_write


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


from backup_wrapper import setup_parser, get_stdout, setup_logging, get_logger, apack



def do_export(
        *,
        path: str,
        backoff: int,
        compression: Optional[str],
        command: Sequence[str],
) -> None:
    logger = get_logger()
    assert len(command) > 0
    commands = ' '.join(command) # TODO use check_call instead?

    stdout = get_stdout(command=commands, backoff=backoff, compression=compression)

    path = replace_placeholders(path)

    logger.debug("writing to %s", path)

    with atomic_write(path, mode='wb', overwrite=True) as fo:
        fo.write(stdout)



def main():
    setup_logging()

    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # TODO FIXME add placeholders doc
    p.add_argument('--path', type=str, help="""
Path with borg-style placeholders

(see https://manpages.debian.org/testing/borgbackup/borg-placeholders.1.en.html)
""")
    # TODO add argument to treat path as is?
    setup_parser(p)

    p.add_argument('command', nargs=argparse.REMAINDER)

    args = p.parse_args()

    path = args.path
    # we want to timestamp _after_ we ran the command
    # s othis is early check for pattern validity, before running the command
    replace_placeholders(path)

    command = args.command
    # https://stackoverflow.com/questions/25872515/python-argparse-treat-arguments-in-different-ways#comment52606932_25873028
    if command[0] == '--':
        del command[0]

    do_export(path=path, backoff=args.backoff, compression=args.compression, command=command)


if __name__ == '__main__':
    main()


def test(tmp_path):
    tdir = Path(tmp_path)
    bdir = tdir.joinpath('backup')
    bdir.mkdir()

    def run(**kwargs):
        do_export(
            path=str(bdir / 'test_{utcnow}_{hostname}.txt'),
            backoff=1,
            # TODO test backoff?
            command=['printf', '0' * 1000],
            **kwargs,
        )

    run(compression=None)
    [ff] = list(bdir.glob('*.txt'))
    assert ff.stat().st_size == 1000

    run(compression='xz')
    [xz] = list(bdir.glob('*.txt')) # TODO not sure, think if we want to automatically compress?
    assert xz.stat().st_size == 76

