(ns clj-gatling.simulation
  (:require [clj-gatling.httpkit :as http]
            [clj-gatling.simulation-runners :refer :all]
            [clj-gatling.schema :as schema]
            [clj-gatling.simulation-util :refer [weighted-scenarios
                                                 choose-runner
                                                 log-exception]]
            [schema.core :refer [check validate]]
            [clj-time.local :as local-time]
            [clojure.set :refer [rename-keys]]
            [clojure.core.async :as async :refer [go go-loop close! put! <!! alts! <! >!]]))

(set! *warn-on-reflection* true)

(defn- now [] (System/currentTimeMillis))

(defn asynchronize [f ctx]
  (let [parse-response (fn [result]
                         (if (vector? result)
                           {:result (first result) :end-time (now) :context (second result)}
                           {:result result :end-time (now) :context ctx}))]
    (go
      (try
        (let [result (f ctx)]
          (if (instance? clojure.core.async.impl.channels.ManyToManyChannel result)
            (parse-response (<! result))
            (parse-response result)))
        (catch Exception e
          {:result false :end-time (now) :context ctx :exception e})))))

(defn async-function-with-timeout [step timeout sent-requests user-id original-context]
  (swap! sent-requests inc)
  (go
    (when-let [sleep-before (:sleep-before step)]
      (<! (async/timeout (sleep-before original-context))))
    (let [original-context-with-user (assoc original-context :user-id user-id)
          start (now)
          return {:name (:name step)
                  :id user-id
                  :start start
                  :context-before original-context-with-user}
          response (asynchronize (:request step) original-context-with-user)
          [{:keys [result end-time context exception]} c] (alts! [response (async/timeout timeout)])]
      (if (= c response)
        [(assoc return :end end-time
                       :exception exception
                       :result result
                       :context-after context) context]
        [(assoc return :end (now)
                       :exception exception
                       :return false
                       :context-after original-context-with-user)
         original-context-with-user]))))

(defn- response->result [scenario result]
  {:name (:name scenario)
   :id (:id (first result))
   :start (:start (first result))
   :end (:end (last result))
   :requests result})

(defn- next-step [[steps step-fn] context]
  (cond
    (seq steps)
    [(first steps) context [(rest steps) nil]]

    (ifn? step-fn)
    (let [result (step-fn context)
          ret (if (vector? result)
                result
                [result context])]
      (conj ret [nil step-fn]))

    :else
    [nil context [nil nil]]))

(defn- run-scenario-once [{:keys [runner simulation-start] :as options}
                          {:keys [pre-hook post-hook] :as scenario} user-id]
  (let [timeout (:timeout-in-ms options)
        sent-requests (:sent-requests options)
        result-channel (async/chan)
        skip-next-after-failure? (if (nil? (:skip-next-after-failure? scenario))
                                   true
                                   (:skip-next-after-failure? scenario))
        should-terminate? #(and (:allow-early-termination? scenario)
                                (not (continue-run? runner @sent-requests simulation-start)))
        request-failed? #(not (:result %))
        merged-context (or (merge (:context options) (:context scenario)) {})
        final-context (if pre-hook
                        (pre-hook merged-context)
                        merged-context)
        step-ctx [(:steps scenario) (:step-fn scenario)]]
    (go-loop [[step context step-ctx] (next-step step-ctx final-context)
              results []]
             (let [[result new-ctx] (<! (async-function-with-timeout step
                                                                     timeout
                                                                     sent-requests
                                                                     user-id
                                                                     context))
                   [step' _ _ :as next-steps] (next-step step-ctx new-ctx)]
               (when-let [e (:exception result)]
                 (log-exception (:error-file options) e))
               (if (or (should-terminate?)
                       (nil? step')
                       (and skip-next-after-failure?
                            (request-failed? result)))
                 (do
                   (when post-hook
                     (post-hook context))
                   (>! result-channel (->> (dissoc result :exception)
                                           (conj results))))
                 (recur next-steps (conj results result)))))
    result-channel))

(defn- scenario-deficit
  [{:keys [concurrency
           concurrency-distribution
           concurrent-scenarios
           runner
           simulation-start
           sent-requests
           context]}]
  (let [progress (calculate-progress runner @sent-requests simulation-start)
        target-concurrency (* concurrency (concurrency-distribution progress context))
        deficit ( - target-concurrency @concurrent-scenarios)]
    deficit))

(defn- run-scenario-constantly
  [{:keys [concurrent-scenarios
           runner
           sent-requests
           simulation-start
           concurrency
           concurrency-distribution
           context] :as options}
   scenario
   user-id]
  (let [c (async/chan)
        should-run-now? (if concurrency-distribution
                          #(pos? (scenario-deficit options))
                          (constantly true))]
    (go-loop []
             (if (should-run-now?)
               (do
                 (swap! concurrent-scenarios inc)
                 (let [result (<! (run-scenario-once options scenario user-id))]
                   (swap! concurrent-scenarios dec)
                   (>! c result)))
               (<! (async/timeout 200)))
             (if (continue-run? runner @sent-requests simulation-start)
               (recur)
               (close! c)))
    c))

(defn- print-scenario-info [scenario]
  (println "Running scenario" (:name scenario)
           "with concurrency" (count (:users scenario))))

(defn- ramp-up-scenario
  [{:keys [concurrency
           runner
           sent-requests
           simulation-start
           context]
    :as options}
   scenario]

  (let [max-concurrent-timeouts 1024
        results (async/chan)
        users (:users scenario)
        init-ch (async/chan)]
    (go-loop [users users
              users-ch init-ch]
             (if (seq users)
               (let [n (min max-concurrent-timeouts (int (scenario-deficit options)))
                     users-left (if (pos? n) (drop n users) users)
                     users-ch (if (pos? n)
                                (let []
                                  (async/merge
                                    (conj (map (partial run-scenario-constantly options scenario) (take n users))
                                          users-ch)))
                                users-ch)]

                 ; feed-out results during ramp-up period
                 (let [timeout-ch (async/timeout 20)]
                   (async/alt!
                     timeout-ch nil
                     users-ch ([v _] (>! results v))))
                 (recur users-left users-ch))
               (do
                 (async/close! init-ch)
                 (async/pipe users-ch results))))
    results))

(defn- run-scenario [{:keys [concurrency-distribution] :as options} scenario]
  (print-scenario-info scenario)
  (let [concurrent-scenarios (atom 0)
        responses (if concurrency-distribution
                    (ramp-up-scenario (assoc options :concurrent-scenarios concurrent-scenarios) scenario)
                    (async/merge (map #(run-scenario-constantly (assoc options :concurrent-scenarios concurrent-scenarios) scenario %) (:users scenario))))
        results (async/chan)]
    (go-loop []
             (if-let [result (<! responses)]
               (do
                 (>! results (response->result scenario result))
                 (recur))
               (close! results)))
    results))

(defn run-scenarios [{:keys [post-hook context runner concurrency-distribution] :as options} scenarios]
  (println "Running simulation with"
           (runner-info runner)
           (if concurrency-distribution
             "using concurrency distribution function"
             ""))
  (validate [schema/RunnableScenario] scenarios)
  (let [simulation-start (local-time/local-now)
        sent-requests (atom 0)
        run-scenario-with-opts (partial run-scenario
                                        (assoc options
                                               :simulation-start simulation-start
                                               :sent-requests sent-requests))
        responses (async/merge (map run-scenario-with-opts scenarios))
        results (async/chan)]
    (go-loop []
             (if-let [result (<! responses)]
               (do
                 (>! results result)
                 (recur))
               (do
                (close! results)
                (when post-hook (post-hook context)))))
    results))

(defn run [{:keys [scenarios pre-hook post-hook] :as simulation}
           {:keys [concurrency users context] :as options}]
  (validate schema/Simulation simulation)
  (let [user-ids (or users (range concurrency))
        final-ctx (merge context (when pre-hook (pre-hook context)))]
    (run-scenarios (assoc options
                          :context final-ctx
                          :post-hook post-hook
                          :runner (choose-runner scenarios
                                                 (count user-ids)
                                                 options))
                   (weighted-scenarios user-ids scenarios))))
