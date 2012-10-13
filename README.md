cc_homework
===========

* Crawler
  Crawler is a basic concurrent tool to crawl web pages.
  By default it craws web pages into the following positions:
  - /revind/in in HDFS stores all web pages with file name being base
    64 encoded url
  - In HBase, table named *WebData* with *url* as its key and
    storage family *content*. Currently has quanlifier *title* storing
    the *title* section of the web page.

* Hind
  Hind is a reverse index builder with lucene smart chinese analyzer
  to tokenize sentences. It takes in every file inside /revind/in by
  default and outputs a reverse index to /revind/out and at the same
  time saving a record into HBase with *word* as its key and an array
  that resides in *revind:array* representing an array of
  \[\["url" "frequency"\]\].
