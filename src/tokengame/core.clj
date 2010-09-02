(ns tokengame.core
  (:use [tokengame.petri]
        [tokengame.pnml]
        [tokengame.nodes]
        [clojure.main])
  (:gen-class :main true))

(defn cmd-param-to-keyword
  "Transforms a command line argument (-something) into a keyword (:something)"
  ([atom]
     (if (keyword? atom)
       atom
       (if (.startsWith atom "-") (keyword (.substring atom 1)) atom))))

(defn cmd-params-to-hash
  ([args]
     (apply hash-map (map cmd-param-to-keyword args))))

(defn find-transition
  ([model name]
;     (println (str "Checking name " name  "\n\n in MODEL: " model))
     (first (filter #(= (:name %1) name) (:transitions model)))))

(defn find-place
  ([model name]
;     (println (str "Checking name " name  "\n\n in MODEL: " model))
     (first (filter #(= (:name %1) name) (:places model)))))

(defn process-command [model command args]
  (condp = command
    "run-transition" (let [name (first args)
                           rabbit-args (cmd-params-to-hash (rest args))]
                       (when (or (nil? name) (nil? (find-transition model name))) (throw (Exception. (str "Cannot find transition named " name " in model " model))))
                       (start-framework rabbit-args)
                       (run (find-transition model name)))

    "watch-place" (let [name (first args)
                        rabbit-args (cmd-params-to-hash (rest args))]
                    (when (or (nil? name) (nil? (find-place model name))) (throw (Exception. (str "Cannot find place named " name " in model " model))))
                    (start-framework rabbit-args)
                    (run-sink (find-place model name) (fn [queue] (do (println (str "*** TOKEN - " (java.util.Date.) " : " (first queue)))
                                                                      (recur (rest queue))))))
    "fire" (let [name (first args)
                 token-code (second args)
                 rabbit-args (cmd-params-to-hash (rest (rest args)))]
             (when (or (nil? name) (nil? (find-place model name))) (throw (Exception. (str "Cannot find place named " name " in model " model))))
             (start-framework rabbit-args)
             (run-fire (find-place model name) (eval (read-string token-code)))
             (java.lang.System/exit 0))

    "run-standalone" (let [rabbit-args (cmd-params-to-hash args)]
                       (start-framework rabbit-args)
                       (run-net-locally model))

    "log" (let [rabbit-args (cmd-params-to-hash (rest args))]
            (start-framework rabbit-args)
            (tail-log))

    (println (str "unknown command" command))))

(defn show-help []
  (println "tokengame syntax: java -cp app.jar tokengame.core [model-file.pnml | node-config.clj] COMMAND [ARGS]")
  (println "COMMAND: petri run-transition name [-username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]")
  (println "COMMAND: petri watch-place place-name [-username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]")
  (println "COMMAND: petri fire place-name token-code [-username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]")
  (println "COMMAND: petri run-standalone [-username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]")
  (println "COMMAND: petri log [-username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]")
  (println "COMMAND: node start"))

(defn -main
  [& args]
  (println (str "ARGS: " args))
  (let [min-num-args (condp = (nth args 2)
                       "run-standalone" 3
                       "log"            3
                       "start"          3
                       4)]
    (if (< (count args) min-num-args)
      (show-help)
      (let [file (first args)
            kind (second args)]
        (if (= kind "node")
          (let [config (eval (read-string (slurp file)))]
            (bootstrap-node (:node-name config) (:rabbit-options config) (:zookeeper-options config))
            (spawn-in-repl)
            (repl))
          (let [command-args (rest (rest args))
                pnml-file file
                model (parse-pnml (java.io.File. pnml-file))]
            (println (str "*** Loaded model: " model))
            (process-command model (first command-args) (rest command-args))))))))
