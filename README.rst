..
    TroveHarvester - A tool for harvesting digitised newspaper articles from Trove

    Written in 2016 by Tim Sherratt tim@discontents.com.au

    To the extent possible under law, the author(s) have dedicated all copyright and related and neighboring rights to this software to the public domain worldwide. This software is distributed without any warranty.

    You should have received a copy of the CC0 Public Domain Dedication along with this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.

TroveHarvester 
==============

This is a tool for harvesting large quantities of digitised newspaper articles from `Trove <http://trove.nla.gov.au>`_.

It has been tested on MacOSX and Windows 7, and should work ok with Python 2.7 and Python 3.

Installation
------------

Assuming you have Python and `Virtualenv <https://virtualenv.pypa.io/en/latest/>`_ installed just:

.. code-block:: bash

    $ virtualenv mytroveharvests
    $ cd mytroveharvests
    $ source bin/activate
    $ pip install troveharvester

On Windows it should be:

.. code-block:: bash

    > virtualenv mytroveharvests
    > cd mytroveharvests
    > Scripts\activate
    > pip install troveharvester

Basic usage
-----------

Before you do any harvesting you need to get yourself a `Trove API key <http://help.nla.gov.au/trove/building-with-trove/api>`_.

There are three basic commands:

* **start** -- start a new harvest
* **restart** -- restart a stalled harvest
* **report** -- view harvest details

Start a harvest
---------------

To start a new harvest you can just do:

.. code-block:: bash

    $ cd mytroveharvests
    $ source bin/activate
    $ troveharvester start "[Trove query]" [Trove API key]

Or on Windows:

.. code-block:: bash

    > cd mytroveharvests
    > Scripts\activate
    > troveharvester start "[Trove query]" [Trove API key]

The Trove query can either be a url copy and pasted from a search in the `Trove web interface <http://trove.nla.gov.au/newspaper/>`_, or a Trove API query url constructed using something like the `Trove API Console <https://troveconsole.herokuapp.com/>`_. Enclose the url in double quotes.

A  ``data`` directory will be automatically created to hold all of your harvests. Each harvest will be saved into a directory named with a current timestamp. Details of harvested articles are written to a CSV file named ``results.csv``. The harvest configuration details are also saved to a ``metadata.json`` file.

Options:

--max [integer]
    specify a maximum number of articles to harvest (multiples of 20)

\--pdf
    save a copy of each each as a PDF (this makes the harvest a *lot* slower as you have to allow a couple of seconds for each PDF to generate)

\--text
    save the OCRd text of each article into a separate ``.txt`` file

Restart a harvest
-----------------

Things go wrong and harvests get interrupted. If your harvest stops before it should, you can just do:

.. code-block:: bash

    $ troveharvester restart

By default the script will try to restart the most recent harvest. You can also restart an earlier harvest:

.. code-block:: bash

    $ troveharvester restart --harvest [harvest timestamp]

Get a summary of a harvest
--------------------------

If you'd like to quickly check the status of a harvest, just try:

.. code-block:: bash

    $ troveharvester report

By default the script will report on the most recent harvest. You can get a summary for an earlier harvest:

.. code-block:: bash

    $ troveharvester report --harvest [harvest timestamp]


