from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='lcmap-firebird',
      version='2017.04.11',
      description='Apache Spark based product generation for LCMAP',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Unlicense',
        'Programming Language :: Python :: 3.5',
      ],
      keywords='usgs lcmap eros spark pyccd',
      url='http://github.com/usgs-eros/lcmap-firebird',
      author='USGS EROS LCMAP',
      author_email='',
      license='Unlicense',
      packages=['firebird'],
      install_requires=[
          'gdal',
          'cytoolz',
          'lcmap-merlin==1.0rc3',
      ],
      # List additional groups of dependencies here (e.g. development
      # dependencies). You can install these using the following syntax,
      # for example:
      # $ pip install -e .[test]
      extras_require={
          'test': ['pytest',
                   'hypothesis',
                   'mock',
                   'lcmap-pyccd==2017.06.20',
                  ],
          'dev': ['jupyter',],
      },
      #test_suite='nose.collector',
      #tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': ['firebird=firebird.cmdline:cli'],
      },
      include_package_data=True,
      zip_safe=False)
