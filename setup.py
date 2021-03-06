#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0', 'colossus', 'astropy', 'pandas', 'scikit-learn',
                'numpy', 'scipy', 'dill', 'emcee', 'tqdm', 'multiprocess',
                'dask', 'pyspark', 'pymangle'
                ]

dependency_links = [
    # Make sure to include the `#egg` portion so the `install_requires`
    # recognizes the package
    # Dask friendly version of emcee.
    'git+ssh://git@github.com/jacobic/emcee.git#egg=emcee-emcee-2.2.1'
#
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(author="Jacob Ider Chitham", author_email='jacobic@mpe.mpg.de',
    classifiers=['Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License', 'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7', ],
    description="Python Boilerplate contains all the boilerplate you need to "
                "create a Python package.",
    entry_points={
        'console_scripts': ['spiders=spiders.cli:main', ], },
    install_requires=requirements, license="MIT license",
    long_description=readme + '\n\n' + history, include_package_data=True,
    keywords='spiders', name='spiders',
    packages=find_packages(include=['spiders', 'spiders.*']),
    setup_requires=setup_requirements, test_suite='tests',
    tests_require=test_requirements, url='https://github.com/jacobic/spiders',
    version='0.1.0', zip_safe=False, )
