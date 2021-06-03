(ns clj-gatling.simulation-util
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]
           [java.io StringWriter PrintWriter]
           [java.time Duration]
           [clj_gatling.simulation_runners FixedRequestNumberRunner DurationRunner]))

(defn create-dir [^String dir]
  (.mkdirs (File. dir)))

(defn append-file
  "Append `contents` to file at `path`. The file is created
  if it doesn't already exist."
  [^String path contents]
  (with-open [w (io/writer path :append true)]
    (.write w contents)))

(defn path-join [& paths]
  (.getCanonicalPath (apply io/file paths)))

(defn exception->str
  "Convert an exception object to a string representation."
  [^Exception e]
  (let [sw (StringWriter.)
        pw (PrintWriter. sw)]
    (.printStackTrace e pw)
    (.toString sw)))

(defn log-exception
  "Log exception `e` to file at `path`."
  [path e]
  (append-file path (exception->str e)))

(defn- distinct-request-count [scenarios]
  (reduce +
          (map #(max (count (:steps %))
                     (count (:requests %))) ;For legacy support
               scenarios)))

(defn- split-by-weight [total weights]
  (let [sum-weights (reduce + weights)
        ;We first try to split mathematically with rounding error
        ;And then later adding reminder to first parts
        ;This means we get a bit different results depending on the order of weights
        ;(split-by-weight 10 [1 1 2]) -> [3 2 5]
        ;(split-by-weight 10 [2 1 1]) -> [6 2 2]
        ;In this tool this is not an issue
        xs (map #(max 1 (int (* total (/ % sum-weights)))) weights)
        mismatch (- total (reduce + xs))]
    (map #(+ %1 %2) xs (concat (repeat mismatch 1) (repeat total 0)))))

(defn split-to-buckets [ids bucket-sizes]
  (loop [start-idx 0
         result []
         sizes bucket-sizes]
    (if (empty? sizes)
      result
      (recur (+ start-idx (first sizes))
             (conj result (subvec ids start-idx (+ start-idx (first sizes))))
             (drop 1 sizes)))))

;https://stackoverflow.com/questions/10969708/parallel-doseq-for-clojure
(defn split-equally
  "Split a collection into a vector of (as close as possible) equally sized parts"
  [size coll]
  (loop [size size
         parts []
         coll coll
         c (count coll)]
    (if (<= size 0)
      parts
      (let [t (quot (+ c size -1) size)]
        (recur (dec size) (conj parts (take t coll)) (drop t coll) (- c t))))))

(defn split-number-equally
  "Split a number into a vector of (as close as possible) equally sized numbers"
  [size number]
  (loop [size size
         parts []
         number number]
    (if (<= size 0)
      parts
      (let [t (quot (+ number size -1) size)]
        (recur (dec size) (conj parts t) (- number t))))))

(defn weighted-scenarios [users scenarios]
  {:pre [(>= (count users) (count scenarios))]}
  (let [weights            (map #(or (:weight %) 1) scenarios)
        xs                 (split-to-buckets (vec users)
                                             (split-by-weight (count users) weights))]
    (map #(assoc (dissoc %1 :weight) :users %2)
         scenarios
         xs)))

(defn- convert-joda-duration-to-java-duration [duration]
  (-> duration
      (.toStandardDuration)
      (.getMillis)
      (Duration/ofMillis)))

(defn choose-runner [scenarios concurrency options]
  (let [duration (:duration options)
        requests (or (:requests options) (* concurrency (distinct-request-count scenarios)))]
    (if (nil? duration)
      (FixedRequestNumberRunner. requests)
      (if (instance? Duration duration)
        (DurationRunner. duration)
        (DurationRunner. (convert-joda-duration-to-java-duration duration))))))

(defn timestamp-str []
  (let [custom-formatter (f/formatter "yyyyMMddHHmmssSSS")]
    (f/unparse custom-formatter (t/now))))

(defn create-report-name
  "Create a gatling compatible filename for report output: 'SimulationName-Timestamp'"
  [simulation-name]
  (let [sanitized-prefix (str/replace (or simulation-name "empty_name") #"[^a-zA-Z0-9_]" "")]
    (str sanitized-prefix "-" (timestamp-str))))

(defn symbol-namespace [^clojure.lang.Symbol simulation]
  (str "/"
       (clojure.string/join "/"
                            (-> simulation
                                (str)
                                (clojure.string/replace #"\." "/")
                                (clojure.string/replace #"-" "_")
                                (clojure.string/split #"/")
                                (drop-last)))))

(defn load-namespace [^clojure.lang.Symbol simulation]
  (let [loadable-ns (symbol-namespace simulation)]
    (when (not (= "/" loadable-ns)) ;No need to load if namespace is current ns
      (load loadable-ns))))

(defn eval-if-needed [instance-or-symbol]
  (if (symbol? instance-or-symbol)
    (let [loadable-ns (symbol-namespace instance-or-symbol)]
      (when (not (= "/" loadable-ns)) ;No need to load if namespace is current ns
        (load loadable-ns))
      (eval instance-or-symbol))
    instance-or-symbol))
