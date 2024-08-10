from setuptools import setup


def main() -> None:
    name = 'arctee'
    setup(
        name=name,
        zip_safe=False,
        py_modules=[name],
        install_requires=[
            'atomicwrites',
        ],
        extras_require={
            'backoff': ['backoff'],  # TODO switch to tenacity?
        },
        entry_points={'console_scripts': ['arctee = arctee:main']},
    )


if __name__ == "__main__":
    main()
