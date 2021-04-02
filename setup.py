from setuptools import setup


def main():
    name = 'arctee'
    setup(
        name=name,
        zip_safe=False,
        py_modules=[name],
        install_requires=['atomicwrites'],
        entry_points={'console_scripts': ['arctee = arctee:main']},
    )


if __name__ == "__main__":
    main()
