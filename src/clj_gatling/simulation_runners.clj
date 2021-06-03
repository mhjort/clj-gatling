(ns clj-gatling.simulation-runners
  (:require [clj-time.core :as time]
            [clj-time.local :as local-time])
  (:import (java.time Duration LocalDateTime)))

(set! *warn-on-reflection* true)

(defprotocol RunnerProtocol
  (calculate-progress [runner sent-requests start])
  (continue-run? [runner sent-requests start])
  (runner-info [runner]))

(deftype DurationRunner [duration]
  RunnerProtocol
  (calculate-progress [runner sent-requests start]
    (let [now (local-time/local-now)
          time-taken-in-secs (time/in-seconds (time/interval start now))
          duration-in-secs (max (time/in-seconds duration) 1)]
      (float (/ time-taken-in-secs duration-in-secs))))
  (continue-run? [runner _ start]
    (.isBefore (LocalDateTime/now)
                  (.plus start duration)))
  (runner-info [_] (str "duration " duration)))

(deftype FixedRequestNumberRunner [number-of-requests]
  RunnerProtocol
  (calculate-progress [runner sent-requests _]
    (float (/ sent-requests number-of-requests)))
  (continue-run? [runner sent-requests _]
    (< sent-requests number-of-requests))
  (runner-info [_] (str "number of requests " number-of-requests)))

(-> (clj-time.core/minutes 5)
    (.toStandardDuration)
    .getMillis)
    ;(.getMillis (.toStandardDuration )

