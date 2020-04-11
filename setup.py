from setuptools import setup, find_packages


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="avro_models",
    version="1.0.0",
    author="Kun Fang",
    author_email="kfang_anqing@yahoo.com",
    description="Object-relational mapping for AVRO schema",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kun-fang/avro-data-model",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords="avro avro-schema orm python3 avro-data data-models",
    install_requires=[
        "avro-python3>=1.8.2",
        "six>=1.11.0"
    ]
)
