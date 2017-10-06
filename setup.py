from setuptools import setup
import unittest


def my_test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('test_strix', pattern='test_*.py')
    return test_suite

setup(
    name='strix',
    version='0.1',
    description='TODO',
    url='TODO',
    author='Språkbanken',
    author_email='sb-strix@svenska.gu.se',
    license='MIT',
    packages=['strix','strix.pipeline', 'strix.api'],
    zip_safe=False,
    install_requires=[
        'elasticsearch==5.4.0',
        'elasticsearch-dsl==5.3.0',
        'Flask==0.12.2',
        'gevent==1.2.2',
        'requests==2.18.3',
        'Markdown==2.6.8',
        'PyYAML==3.12'
      ],
    test_suite='setup.my_test_suite',
    tests_require=['pytest==2.9.1']
)
