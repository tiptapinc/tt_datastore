from setuptools import setup

setup(
    name='tt_datastore',
    description='TipTap datastore library.',
    long_description=(
        '%s\n\n%s' % (
            open('README.md').read(),
            open('CHANGELOG.md').read()
        )
    ),
    version=open('VERSION').read().strip(),
    author='TipTap',
    install_requires=['couchbase'],
    package_dir={'tt_datastore': 'src'},
    packages=['tt_datastore']
)
