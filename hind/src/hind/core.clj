(ns hind.core
  (:require [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.defjob :as defjob]
            [clojure-hadoop.imports :as imp]
            [clojure-hadoop.config :as config]
            [clojure-hbase.core :as hb])
  (:import (java.util StringTokenizer)
           [org.apache.hadoop.hbase.mapreduce TableInputFormat]
           [org.apache.hadoop.hbase.io ImmutableBytesWritable]
           [org.apache.hadoop.hbase.client Result])
  (:use clojure.test clojure-hadoop.job hind.token))

(imp/import-io)
(imp/import-mapreduce)
(imp/import-mapreduce-lib-input)

(defn my-map [key value]
  (map #(vector % key) (hind.token/tokenize (.toString value))))

(defn my-reduce [key values-fn]
  [[key (clojure.string/join
         (reduce #(assoc %1 %2 (inc (get %1 %2 0))) {} (values-fn)))]])

(defn table-map-reader [^ImmutableBytesWritable rowKey ^Result result]
  [(String. (.getKey rowKey))
   (Text. (.getValue result (.getBytes "text") (.getBytes "clean")))])

(defn string-long-writer [^TaskInputOutputContext context ^String key value]
  (.write context (Text. key) value))

(defn string-reduce-writer [^TaskInputOutputContext context ^String key value]
  (hb/with-table [index-data (hb/table "IndexResult")]
    (hb/put index-data key :values [:revind [:array (str value)]]))
  (.write context (Text. key) (Text. value)))

(defn string-long-reduce-reader [^Text key wvalues]
  [(.toString key) (fn [] (map (fn [^Text v] (.toString v)) wvalues))])

(defn- set-table-name [^Job job]
  (.set (config/configuration job) TableInputFormat/INPUT_TABLE "WebData"))

(defjob/defjob job
  :name "WebData"
  :configure set-table-name
  :map my-map :map-reader table-map-reader
  :map-writer string-long-writer
  :reduce my-reduce
  :reduce-reader string-long-reduce-reader
  :reduce-writer string-reduce-writer
  :output-key Text
  :output-value Text
  :input-format TableInputFormat
  :output-format "text"
  :compress-output false
  :input "WebData"
  :output "/revind/out"
  :replace true)
