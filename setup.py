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
    install_requires=[
        'aiofiles==0.4.0',
        'aiohttp==3.5.1',
        'async-timeout==3.0.1',
        'atomicwrites==1.2.1',
        'attrs==18.2.0',
        'backcall==0.1.0',
        'certifi==2018.11.29',
        'chardet==3.0.4',
        'decorator==4.3.0',
        'idna==2.8',
        'jedi==0.13.2',
        'more-itertools==4.3.0',
        'multidict==4.5.2',
        'parso==0.3.1',
        'pexpect==4.6.0',
        'pickleshare==0.7.5',
        'pluggy==0.8.0',
        'prompt-toolkit==2.0.7',
        'ptyprocess==0.6.0',
        'py==1.7.0',
        'Pygments==2.3.1',
        'pytest==4.0.2',
        'requests==2.21.0',
        'six==1.12.0',
        'traitlets==4.3.2',
        'urllib3==1.26.5',
        'wcwidth==0.1.7',
        'yarl==1.3.0',
    ]
)