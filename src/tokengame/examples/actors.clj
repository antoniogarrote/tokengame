(ns tokengame.examples.actors
  (:use [tokengame.nodes]
        [matchure]))

(defn ping
  ([]
     (loop [continue true
            msg (receive)]
       (cond-match
        [#"exit" msg] (recur false msg)
        [#"exception" msg] (throw (Exception. "Ping actor received exception"))
        [[?from ?data] msg] (do (send! from data)
                              (recur true (receive)))))))
