from setuptools import setup


setup(
    name='strixpipeline',
    version='0.1',
    description='TODO',
    url='TODO',
    author='Språkbanken',
    author_email='sb-strix@svenska.gu.se',
    license='MIT',
    packages=['strixpipeline'],
    zip_safe=False,
    install_requires=[
        'elasticsearch==5.4.0',
        'elasticsearch-dsl==5.3.0',
        'PyYAML==3.12',
      ]
)
