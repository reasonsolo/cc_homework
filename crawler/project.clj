(defproject crawler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :main crawler.crawl
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [enlive "1.0.1"]
                 [clj-base64 "0.0.2"]
                 [clojure-hbase "0.90.5-4"]
                 [com.cemerick/pomegranate "0.0.13"]
                 [org.apache.hadoop/hadoop-core "1.0.1"]])
