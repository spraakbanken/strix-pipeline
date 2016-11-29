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
    packages=['strix'],
    zip_safe=False,
    install_requires=[
        'click==6.6',
        'elasticsearch-dsl==5.0.0',
        'Flask==0.11.1',
        'Flask-Compress==1.3.0',
        'Flask-Cors==2.1.2',
        'itsdangerous==0.24',
        'Jinja2==2.8',
        'MarkupSafe==0.23',
        'python-dateutil==2.5.2',
        'six==1.10.0',
        'urllib3 ==1.16',
        'waitress==0.9.0b0',
        'Werkzeug==0.11.10',
        'requests==2.12.1'
      ],
    test_suite='setup.my_test_suite',
    tests_require=['pytest==2.9.1']
)
