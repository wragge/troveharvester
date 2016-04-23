from setuptools import setup

setup(name='troveharvester',
      version='0.1.0',
      packages=['troveharvester'],
      description='Harvester for Trove newspaper articles.'
      long_description='This is a tool for harvesting large collections of digitised newspaper articles from Trove (http://trove.nla.gov.au).'
      entry_points={
          'console_scripts': [
              'troveharvester = troveharvester.__main__:main'
          ]
      },
      )