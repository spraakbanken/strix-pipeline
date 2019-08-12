from setuptools import setup


setup(
    name='strixpipeline',
    version='1.5.dev',
    description='',
    url='',
    author='Språkbanken',
    author_email='sb-strix@svenska.gu.se',
    license='MIT',
    packages=['strixpipeline'],
    zip_safe=False,
    install_requires=[
        'elasticsearch==7.0.2',
        'elasticsearch-dsl==7.0.0',
        'PyYAML==5.1.2',
      ]
)
