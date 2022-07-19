from setuptools import setup
from Cython.Build import cythonize
import numpy as np
setup(
    name='Series working quickly',
    ext_modules=cythonize("work_dataframes.pyx"),
    include_dirs=[np.get_include()],
)
