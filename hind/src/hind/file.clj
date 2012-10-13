(ns hind.file
  (:require [clojure-hadoop.wrap :as wrap]
            [clojure-hadoop.defjob :as defjob]
            [clojure-hadoop.imports :as imp])
  (:import (java.util StringTokenizer)
           (java.net URLDecoder))
  (:use clojure.test clojure-hadoop.job hind.token))

(imp/import-io)
(imp/import-mapreduce)
(imp/import-mapreduce-lib-input)

(defn my-map [key value]
  (map #(vector % 1) (hind.token/tokenize (.toString value))))

(defn my-reduce [key values-fn]
  [[key (clojure.string/join
         (reduce #(assoc %1 %2 (inc (get %1 %2 0))) {} (values-fn)))]])

(defn string-long-writer [^TaskInputOutputContext context ^String key value]
  (.write context (Text. key) (->> context .getInputSplit (cast FileSplit)
                                   .getPath .toString URLDecoder/decode Text.)))

(defn string-reduce-writer [^TaskInputOutputContext context ^String key value]
  (.write context (Text. key) (Text. value)))

(defn string-long-reduce-reader [^Text key wvalues]
  [(.toString key) (fn [] (map (fn [^Text v] (.toString v)) wvalues))])

(defjob/defjob job
  :map my-map :map-reader wrap/int-string-map-reader
  :map-writer string-long-writer :reduce my-reduce
  :reduce-reader string-long-reduce-reader :reduce-writer string-reduce-writer
  :output-key Text :output-value Text :input-format "text" :output-format "text"
  :compress-output false :input "test/in/" :output "test/out" :replace true)
