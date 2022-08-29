from setuptools import setup

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='troveharvester',
      version='0.5.1',
      packages=['troveharvester'],
      description='Tool for harvesting Trove digitised newspaper articles.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Tim Sherratt',
      author_email='tim@discontents.com.au',
      licence='CC0',
      url='https://github.com/wragge/troveharvester',
      install_requires=['pandas', 'requests', 'arrow', 'tqdm', 'pillow', 'bs4', 'lxml', 'html2text', 'trove-query-parser', 'trove-newspaper-images'],
      entry_points={
          'console_scripts': [
              'troveharvester = troveharvester.__main__:main'
          ]
      },
      )
