from setuptools import setup


setup(
    name="strixpipeline",
    description="",
    url="",
    author="Språkbanken",
    author_email="sb-strix@svenska.gu.se",
    license="MIT",
    packages=["strixpipeline"],
    zip_safe=False,
    install_requires=[
        "elasticsearch==8.15.1",
        "elasticsearch-dsl==8.12.0",
        "PyYAML==6.0.1",
        "ruff==0.5.7",
        "orjson==3.10.11",
    ],
)
