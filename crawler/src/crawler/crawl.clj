(ns crawler.crawl
  (:gen-class :main true)
  (:require [net.cgrand.enlive-html :as enlive]
            [clojure-hbase.core :as hb])
  (:use [clojure.string :only (lower-case join)]
        [clojure.java.io :only (as-url)]
        [crawler.hdfs :only (write-string *base-url*)]
        [remvee.base64])
  (:import [java.net URL MalformedURLException]
           [java.util.concurrent LinkedBlockingQueue BlockingQueue]))

(defn- links-from
  [base-url html]
  (remove nil? (for [link (enlive/select html [:a])]
                 (when-let [href (-> link :attrs :href)]
                   (try
                     (URL. base-url href)
                     (catch MalformedURLException e))))))

(defn- words-from
  [html]
  (let [chunks (-> html
                   (enlive/at [:script] nil)
                   (enlive/at [:style] nil)
                   (enlive/select [:body enlive/text-node]))]
    (join " " (->> chunks
                   (mapcat (partial re-seq #"[\u4e00-\u9fa5]+|\w+"))
                   (remove (partial re-matches #"\d+"))
                   (map lower-case)))))

(def url-queue (LinkedBlockingQueue.))
(def crawled-urls (atom #{}))
(def word-freqs (atom {}))

(declare get-url)

(def agents (set (repeatedly 25 #(agent {::t #'get-url :queue url-queue}))))

(declare run process handle-results)

(defn ^::blocking get-url
  [{:keys [^BlockingQueue queue] :as state}]
  (let [url (as-url (.take queue))]
    (try
      (if (@crawled-urls url)
        state
        {:url url
         :content (slurp url :encoding "GBK")
         ::t #'process})
      (catch Exception e
        state)
      (finally (run *agent*)))))

(defn process
  [{:keys [url content]}]
  (try
    (let [html (enlive/html-resource (java.io.StringReader. content))]
      {::t #'handle-results
       :url url
       :links (links-from url html)
       :text (words-from html)})
    (finally (run *agent*))))

(defn ^::blocking handle-results
  [{:keys [url links text]}]
  (try
    (swap! crawled-urls conj url)
    (doseq [url links]
      (.put url-queue url))
    (write-string
     (str "/revind/in/" (clojure.string/replace
                         (encode-str (.toString url)) #"/" "|"))
     text )
    {::t #'get-url :queue url-queue}
    (finally (run *agent*))))

(defn paused? [agent] (::paused (meta agent)))

(defn run
  ([] (doseq [a agents] (run a)))
  ([a]
     (when (agents a)
       (send a (fn [{transition ::t :as state}]
                 (when-not (paused? *agent*)
                   (let [dispatch-fn (if (-> transition meta ::blocking)
                                       send-off
                                       send)]
                     (dispatch-fn *agent* transition)))
                 state)))))

(defn pause
  ([] (doseq [a agents] (pause a)))
  ([a] (alter-meta! a assoc ::paused true)))

(defn restart
  ([] (doseq [a agents] (restart a)))
  ([a]
     (alter-meta! a dissoc ::paused)
     (run a)))

(defn test-crawler
  [agent-count starting-url wait-time]
  (def agents (set (repeatedly agent-count
                             #(agent {::t #'get-url :queue url-queue}))))
  (.clear url-queue)
  (swap! crawled-urls empty)
  (swap! word-freqs empty)
  (.add url-queue starting-url)
  (run)
  (Thread/sleep wait-time)
  (pause)
  [(count @crawled-urls) (count url-queue)])

(defn -main
 ([]
  (println "Params: <URL> <CONCURRENT> <RUNNING_TIME=60sec>"))
 ([start-url agent-count]
  (-main start-url agent-count 60000))
 ([start-url agent-count wait-time]
    (println (binding [*base-url* "localhost:9000"]
               (test-crawler (Integer. agent-count)
                             start-url
                             (Integer. wait-time))))))
