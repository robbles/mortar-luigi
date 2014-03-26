try:
    from setuptools import setup
except:
    from distutils.core import setup

from distutils.core import setup

try:
    long_description = []
    for line in open('README.md'):
        # Strip all images from the pypi description
        if not line.startswith('!') and not line.startswith('```'):
            long_description.append(line)
except IOError:
    # Temporary workaround for pip install
    long_description = ''

setup(name='mortar-luigi',
      version='0.1.0',
      description='Mortar Extensions for Luigi',
      long_description=long_description,
      author='Mortar Data',
      author_email='info@mortardata.com',
      url='http://github.com/mortardata/mortar-luigi',
      namespace_packages = [
          'mortar'
      ],
      packages=[
          'mortar.luigi'
      ],
      license='LICENSE.txt',
      install_requires=[
          'luigi',
          'requests',
          'boto==2.24.0',
          'pymongo>=2.5',
          'mortar-api-python'
      ],
      test_suite="nose.collector",
      tests_require=[
          'mock',
          'moto',
          'nose==1.3.0'
      ]
)
