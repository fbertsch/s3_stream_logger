from setuptools import setup, find_packages

setup(
    name = 'S3_Stream_Logger',
    version = '0.0.1',
    description = 'Tool for logging a never-ending stream of data to S3',
    py_modules = ['logger'],

    author = u'Frank Bertsch',
    author_email = 'fbertsch@mozilla.com',

    url = 'https://github.com/fbertsch/S3_Stream_Logger',

    setup_requires = ['pytest-runner'],
    install_requires = ['smart_open'],
    tests_require = ['moto', 'pytest']
)
