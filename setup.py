#!/usr/bin/env python

from setuptools import setup, find_packages
from qiniu_storage import VERSION

url="https://github.com/jeffkit/qiniu-storage"

long_description="Django storage power by qiniu store"

setup(name="qiniu-storage",
      version=VERSION,
      description=long_description,
      maintainer="jeff kit",
      maintainer_email="bbmyth@gmail.com",
      url = url,
      long_description=long_description,
      packages=find_packages('.'),
     )


