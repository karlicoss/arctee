#!/usr/bin/env python3
"""
Helper script to run your data exports.

You can read more on how it's used TODO

* Motivation
Minimizing common boilerplate while exporting plaintext data from APIs

- =--path= allows easy timestamping and guarantees atomic writing, so you'd never end up with corrupted exports.
- =--compression= allows to compress simply by passing format. No more tar -zcvf!
- =--retries= allows easy exponential backoff in case service you're querying is flaky.

* Dependencies
- =pip3 install --user atomicwrites=
  [[https://github.com/untitaker/python-atomicwrites][atomicwrites]] is a library for portable atomic file writing
- =pip3 install --user backoff=
  [[https://github.com/litl/backoff][backoff]] is a library to simplify backoff and retrying
- =apt install atools=
  [[https://www.nongnu.org/atool][atool]] is a tool to create archives in any format

* Why do you need a special script for that?

- why not use `date` command for timestamps?

  something like =--path "$(date -Iseconds --utc).json" works, however I need it for *most* of my exports; it ends up polluting my crontabs.

Next,I want to do three independent things in row here.
That sounds like a perfect candidate for pipes, right?
Sadly, there are caveats:

- if one parts of your pipe fail, it doesn't fail everything

  That's a major problem that often leads to unexpected behaviours.

  In bash you can fix this by setting =set -o pipefail=. However:

  - default cron shell is =/bin/sh=. Ok, you can change it to ~SHELL=/bin/bash~, but
  - you can't set ~SHELL="/bin/bash -o pipefail"~

    You'd have to prepend all of your pipes with =set -o pipefail=, which is quite boilerplaty

- you can't use pipes for retrying; you need some wrapper script anyway

  E.g. similar to how you need a wrapper script when you want to stop your program on timeout.

- it's possible to use pipes for atomically writing output to a file, however I haven't found any existing tools to do that

  E.g. I want something like =curl https://some.api/get-data | atomically --path /path/to/data.sjon=.

  If you know any existing tool please let me know!

- it's possible to use pipes for compression

  However due to the above concerns (timestamping/retrying/atomic writing), it has to be part of the script as well.

If you think any of these things can be simplified, I'd be happy to know and remove them in favor of more standard solutions!
"""

import argparse
from pathlib import Path
import logging
from subprocess import Popen, PIPE, run, CalledProcessError, check_output
from typing import Sequence, Optional


from atomicwrites import atomic_write
import backoff # type: ignore


def get_logger():
    return logging.getLogger('exportwrapper')


def setup_logging():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


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
    retrier = backoff.on_exception(backoff.expo, exception=CalledProcessError, max_tries=retries, logger=get_logger())
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

    from shlex import quote # TODO ok, careful about it...

    commands = ' '.join(map(quote, command))  # deliberate shell-like behaviour

    stdout = get_stdout(command=commands, retries=retries, compression=compression)

    path = replace_placeholders(path)

    logger.debug("writing %d bytes to %s", len(stdout), path)

    with atomic_write(path, mode='wb', overwrite=True) as fo:
        fo.write(stdout)


def main():
    setup_logging()

    p = argparse.ArgumentParser(
        description='Wrapper for automating routine for reliable and regular data exports', # TODO link?
        formatter_class=argparse.RawTextHelpFormatter,
    )
    pss = ', '.join("{" + p + "}" for p in PLACEHOLDERS)
    p.add_argument('--path', type=str, required=True, help=f"""
Path with borg-style placeholders. Supported: {pss}.

Example: --path '/exports/pocket/pocket_{{utcnow}}.json'

(see https://manpages.debian.org/testing/borgbackup/borg-placeholders.1.en.html)
""".strip())
    # TODO add argument to treat path as is?
    p.add_argument(
        '-r', '--retries',
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
    # TODO hmm, need to prevent splitting
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


def test(tmp_path):
    bdir = Path(tmp_path)

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


def test_retry(tmp_path):
    """
    Ideally, should fail for a while and then succeed.

    # eh. do not run on ci? not sure..
    """
    bdir = Path(tmp_path)

    from subprocess import check_call

    cmd = [
        __file__,
        '-c', 'xz',
        '--retries', '10',  # TODO
        '--path', str(bdir / 'xxx_{utcnow}.html.xz'),
        '--',
        'bash', '-c', '((RANDOM % 3 == 0)) && cat /usr/share/doc/python3/html/bugs.html',
    ]
    check_call(cmd)


if __name__ == '__main__':
    main()
