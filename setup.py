from setuptools import setup

setup(name='troveharvester',
      version='0.1.5',
      packages=['troveharvester'],
      description='Tool for harvesting Trove digitised newspaper articles.',
      author='Tim Sherratt',
      author_email='tim@discontents.com.au',
      licence='CC0',
      url='https://github.com/wragge/troveharvester',
      install_requires=['unicodecsv'],
      entry_points={
          'console_scripts': [
              'troveharvester = troveharvester.__main__:main'
          ]
      },
      )
