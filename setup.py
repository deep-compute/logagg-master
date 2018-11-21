from setuptools import setup
from setuptools import find_packages

setup(
    name='logagg_master',
    version='0.0.1',
    description='Master API for logagg',
    keywords='logagg',
    author='Deep Compute, LLC',
    author_email='contact@deepcompute.com',
    url='https://github.com/deep-compute/logagg_master/logagg_master',
    license='MIT',
    dependency_links=[
        'https://github.com/deep-compute/nsq_api/nsq_api',
    ],
    install_requires=[
	'basescript==0.2.9',
        'kwikapi==0.4.6',
        'kwikapi-tornado==0.3.3',
        'logagg_utils==0.5.0',
        'ujson==1.35',
        'pymongo==3.7.2',
        'tabulate==0.8.2',
        'diskdict==0.2.4',
    ],
    package_dir={'logagg_cli': 'logagg_cli'},
    packages=find_packages('.'),
    include_package_data=True,
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: MIT License',
    ],
    test_suite='test.suite_maker',
    entry_points={
        'console_scripts': [
            'logagg-master = logagg_master:master_command_main',
            'logagg-cli = logagg_cli:cli_command_main'
        ]
    }
)
