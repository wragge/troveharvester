# TroveHarvester

This is a tool for harvesting large quantities of digitised newspaper articles from [Trove](http://trove.nla.gov.au).

It has been tested on MacOS and Windows 7, and should work ok with Python 3.

## Installation options

### No installation required!

If you want to use the harvester without installing anything, just head over to the [Trove Newspaper Harvester](https://glam-workbench.github.io/trove-harvester/) section in my GLAM Workbench.

### Installation via pip

Assuming you have Python 3 installed just:

``` bash
    $ python3 -m venv mytroveharvests
    $ cd mytroveharvests
    $ source bin/activate
    $ pip install troveharvester
```

## Basic usage

Before you do any harvesting you need to get yourself a [Trove API key](http://help.nla.gov.au/trove/building-with-trove/api).

There are three basic commands:

* **start** -- start a new harvest
* **restart** -- restart a stalled harvest
* **report** -- view harvest details

### Start a harvest

To start a new harvest you can just do:

``` bash
    $ cd mytroveharvests
    $ source bin/activate
    $ troveharvester start "[Trove query]" [Trove API key]
```

Or on Windows:

``` bash
    > cd mytroveharvests
    > Scripts\activate
    > troveharvester start "[Trove query]" [Trove API key]
```

The Trove query can either be a url copy and pasted from a search in the [Trove web interface](http://trove.nla.gov.au/newspaper/), or a Trove API query url constructed using something like the [Trove API Console](https://troveconsole.herokuapp.com/). Enclose the url in double quotes.

A  `data` directory will be automatically created to hold all of your harvests. Each harvest will be saved into a directory named with a current timestamp. Details of harvested articles are written to a CSV file named `results.csv`. The harvest configuration details are also saved to a `metadata.json` file.

Options:

--max [integer]
    specify a maximum number of articles to harvest (multiples of 20)

--pdf
    save a copy of each each as a PDF (this makes the harvest a *lot* slower as you have to allow a couple of seconds for each PDF to generate)

--text
    save the OCRd text of each article into a separate ``.txt`` file

--image
    save an image of each article into a separate ``.jpg`` file (if the article is split over more than one page there will be multiple images)

--include_linebreaks
    preserve linebreaks in saved text files

### Restart a harvest

Things go wrong and harvests get interrupted. If your harvest stops before it should, you can just do:

``` bash
    $ troveharvester restart
```

By default the script will try to restart the most recent harvest. You can also restart an earlier harvest:

``` bash
    $ troveharvester restart --harvest [harvest timestamp]
```

### Get a summary of a harvest

If you'd like to quickly check the status of a harvest, just try:

``` bash
    $ troveharvester report
```

By default the script will report on the most recent harvest. You can get a summary for an earlier harvest:

``` bash
    $ troveharvester report --harvest [harvest timestamp]
```
