(ns hind.token
  (:require [clojure.java.io :as io])
  (:import [org.apache.lucene.analysis TokenStream]
           [org.apache.lucene.analysis.cn.smart SmartChineseAnalyzer]
           [org.apache.lucene.analysis.tokenattributes CharTermAttribute]
           [org.apache.lucene.util Version]))

(defn tokenize [^String str]
  (let [stream (.tokenStream (SmartChineseAnalyzer. Version/LUCENE_36)
                             "" (java.io.StringReader. str))
        term (.addAttribute stream CharTermAttribute)]
    (loop [coll []]
      (if (.incrementToken stream)
        (recur (conj coll (.toString term)))
        coll))))

(defn test-token []
  (binding [*out* (io/writer "/home/bruce/fenci")]
    (println (tokenize "你好!Java是一个非常蛋疼的语言哦！我很不喜欢它"))))