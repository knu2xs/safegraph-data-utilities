from setuptools import find_packages, setup

with open('README.md', 'r') as readme:
    long_description = readme.read()

setup(
    name='sg_data',
    package_dir={"": "src"},
    packages=find_packages('src'),
    version='0.0.0',
    description='A few utilities I created for working with Safegraph data for downloading directly from Amazon S3.',
    long_description=long_description,
    author='Joel McCune',
    license='Apache 2.0',
)
