import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="LogDB",
    version="0.0.1",
    author="Dmitriy Shikhalev",
    author_email="dmitriy.shikhalev@gmail.com",
    description="Column-based simple asyncio DB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dmitriy-shikhalev/LogDB",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU GPL",
        "Operating System :: OS Independent",
    ],
)