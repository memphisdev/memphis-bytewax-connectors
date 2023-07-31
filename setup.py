from pathlib import Path

from setuptools import setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="memphis-bytewax-connectors",
    packages=["memphis", "memphis._internal", "memphis.connectors"],
    version="0.0.1",
    license="Apache-2.0",
    description="Connectors for using Bytewax with Memphis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    readme="README.md",
    author="Memphis.dev",
    author_email="team@memphis.dev",
    url="https://github.com/memphisdev/memphis-bytewax-connectors",
    keywords=["message broker", "devtool", "streaming", "data"],
    install_requires=["asyncio", "nats-py >= 2.3.0", "bytewax >= 0.16.0"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
