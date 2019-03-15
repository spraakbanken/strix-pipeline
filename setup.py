from setuptools import setup


setup(
    name='strixpipeline',
    version='1.4',
    description='',
    url='',
    author='Språkbanken',
    author_email='sb-strix@svenska.gu.se',
    license='MIT',
    packages=['strixpipeline'],
    zip_safe=False,
    install_requires=[
        'elasticsearch==6.2.0',
        'elasticsearch-dsl==6.1.0',
        'PyYAML==3.13',
      ]
)
