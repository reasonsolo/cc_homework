(ns crawler.hdfs
  (:use [clojure.java.io :only [reader input-stream]])
  (:import [org.apache.hadoop.fs FileSystem FSDataOutputStream Path]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io IOUtils]
           [org.apache.hadoop.util Progressable]
           [java.io File InputStream]
           [java.net URI]))

(def ^:dynamic *base-url* "localhost:9000")

(defn make-uri [pattern]
  (URI/create (str "hdfs://" *base-url* pattern)))

(defn make-path [filepath]
  (Path.  (make-uri filepath)))

(defn get-fs [filepath]
  (FileSystem/get (make-uri filepath) (Configuration.)))

(defn write-string [path string]
  (println (str "Writing file: " path))
  (let [fs  (get-fs path)
        out (.create fs (make-path path))]
    (let [wtr (clojure.java.io/writer out)]
      (.write wtr string))
    (.close out)))
