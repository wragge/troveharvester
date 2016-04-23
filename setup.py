from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='troveharvester',
      version='0.1.0',
      packages=['troveharvester'],
      description='Tool for harvesting Trove digitised newspaper articles.',
      long_description=long_description,
      author='Tim Sherratt',
      author_email='tim@discontents.com.au',
      licence='CC0',
      url='https://github.com/wragge/troveharvester',
      entry_points={
          'console_scripts': [
              'troveharvester = troveharvester.__main__:main'
          ]
      },
      )
