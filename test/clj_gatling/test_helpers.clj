(ns clj-gatling.test-helpers
  (:require [clojure.core.async :refer [go <! <!! timeout]]
            [clj-gatling.simulation :as simulation]
            [clj-gatling.simulation-util :refer [choose-runner
                                                 weighted-scenarios
                                                 create-dir]]
            [clojure.java.io :as io]))

(defn- to-vector [channel]
  (loop [results []]
    (if-let [result (<!! channel)]
      (recur (conj results result))
      results)))

(def error-file-path "target/test-results/error.log")

(defn setup-error-file-path [f]
  (let [file (io/file error-file-path)]
    (when (not (.exists file))
      (create-dir (.getParent file))))
  (f))

(defn delete-error-logs []
  (io/delete-file error-file-path))

(defn run-legacy-simulation [scenarios concurrency & [options]]
  (let [step-timeout (or (:timeout-in-ms options) 5000)]
    (-> (simulation/run-scenarios {:runner (choose-runner scenarios concurrency options)
                                   :timeout-in-ms step-timeout
                                   :context (:context options)
                                   :error-file error-file-path}
                                  (weighted-scenarios (range concurrency) scenarios)
                                  true)
        to-vector)))

(defn run-single-scenario [scenario & {:keys [concurrency context timeout-in-ms requests duration users pre-hook]
                                        :or {timeout-in-ms 5000}}]
  (to-vector (simulation/run {:name "Simulation"
                              :pre-hook pre-hook
                              :scenarios [scenario]}
                             {:concurrency concurrency
                              :timeout-in-ms timeout-in-ms
                              :requests requests
                              :duration duration
                              :users users
                              :context context
                              :error-file error-file-path})))

(defn run-two-scenarios [scenario1 scenario2 & {:keys [concurrency requests]}]
  (to-vector (simulation/run {:name "Simulation"
                              :scenarios [scenario1 scenario2]}
                             {:concurrency concurrency
                              :requests requests
                              :timeout-in-ms 5000
                              :error-file error-file-path})))

(defn successful-request [cb context]
  ;TODO Try to find a better way for this
  ;This is required so that multiple scenarios start roughly at the same time
  (Thread/sleep 50)
  (cb true (assoc context :to-next-request true)))

(defn failing-request [cb context] (cb false))

(defn fake-async-http [url callback context]
  (future (Thread/sleep 50)
          (callback (= "success" url))))

(defn get-result [requests request-name]
  (:result (first (filter #(= request-name (:name %)) requests))))

