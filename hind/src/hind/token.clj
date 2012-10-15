(ns hind.token
  (:require [clojure.java.io :as io])
  (:import [org.apache.lucene.analysis TokenStream]
           [org.apache.lucene.analysis.cn.smart SmartChineseAnalyzer]
           [org.wltea.analyzer.lucene IKAnalyzer]
           [org.apache.lucene.analysis.tokenattributes CharTermAttribute]
           [org.apache.lucene.util Version]))

;Version/LUCENE_36
(defn tokenize [^String str]
  (let [stream (.tokenStream (IKAnalyzer. )
                             "" (java.io.StringReader. str))
        term (.addAttribute stream CharTermAttribute)]
    (loop [coll []]
      (if (.incrementToken stream)
        (recur (conj coll (.toString term)))
        coll))))

(defn test-token []
  (println (tokenize "你好!Java是一个非常蛋疼的语言哦！我很不喜欢它")))