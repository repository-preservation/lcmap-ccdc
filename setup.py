from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


def version():
    with open('version.txt') as h:
        return h.read().strip()


setup(name='lcmap-ccdc',
      version=version(),
      description='Apache Spark based product generation for LCMAP',
      long_description=readme(),
      classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Unlicense',
        'Programming Language :: Python :: 3.6',
      ],
      keywords='usgs lcmap eros spark pyccd ccdc',
      url='http://github.com/usgs-eros/lcmap-ccdc',
      author='USGS EROS LCMAP',
      author_email='',
      license='Unlicense',
      packages=['ccdc'],
      install_requires=[
          'click==6.7',
          'lcmap-merlin>=2.2.1',
          'lcmap-pyccd==2018.03.12.dev-ncompare.b2',
      ],
      # List additional groups of dependencies here (e.g. development
      # dependencies). You can install these using the following syntax,
      # for example:
      # $ pip install -e .[test]
      extras_require={
          'test': ['pytest',
                   'vcrpy',
                   'mock',
                  ],
          'dev': ['',],
      },
      #test_suite='nose.collector',
      #tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': ['ccdc=ccdc.cli:entrypoint'],
      },
      include_package_data=True,
      zip_safe=False)
