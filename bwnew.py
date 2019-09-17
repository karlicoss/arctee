#!/usr/bin/env python3
from atomicwrites import atomic_write

_ISO_FORMAT = '%Y%m%dT%H%M%SZ'

def utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime(_ISO_FORMAT)


def hostname() -> str:
    import socket
    return socket.gethostname()


PLACEHOLDERS = {
    'utcnow'  : utcnow,
    'hostname': hostname,
}


def replace_placeholders(s) -> str:
    pdict = {
        k: v() for k, v in PLACEHOLDERS.items()
    }
    return s.format(**pdict)


def main():
    from backup_wrapper import setup_parser, backup, get_stdout, setup_logging, get_logger
    setup_logging()
    logger = get_logger()


    import argparse
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # TODO FIXME add placeholders doc
    # TODO make it positional?
    p.add_argument('--path', type=str, help="""
Path with borg-style placeholders (sadly couldn't come up with anything more standard)
https://github.com/borgbackup/borg/blob/d02356e9c06f980b3d53459c6cc9c264d23d499e/src/borg/helpers/parseformat.py#L205
""")
    # TODO add argument to treat path as is
    setup_parser(p)

    p.add_argument('command', nargs=argparse.REMAINDER)


    args = p.parse_args()

    path = args.path
    replace_placeholders(path) # just for early check
    # TODO hmm, not sure when should it replace...
    # TODO determine compression from extension?

    command = args.command
    # https://stackoverflow.com/questions/25872515/python-argparse-treat-arguments-in-different-ways#comment52606932_25873028
    if command[0] == '--':
        del command[0]

    assert len(command) > 0
    commands = ' '.join(command) # TODO use check_call instead?

    assert args.compression is None # TODO support later..

    stdout = get_stdout(command=commands, backoff=args.backoff, compression=args.compression)

    path = replace_placeholders(path)

    logger.debug("writing to %s", path)

    with atomic_write(path, mode='wb', overwrite=True) as fo:
        fo.write(stdout)




if __name__ == '__main__':
    main()
