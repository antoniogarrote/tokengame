(ns tokengame.petri
  (:use [tokengame.rabbit]
        [tokengame.utils]
        [clojure.contrib.logging :only [log]]
        [clojure.contrib.json]))

(declare remote-log)

(defonce *rabbit-server-petri* nil)

(defonce *places-out* nil)

(defn find-place-out
  ([place-name]
     (if (map? place-name) place-name (first (filter #(= place-name (:name %1)) *places-out*)))))

(defn declare-log-exchange
  "Declares a log queue to be used by the whole net"
  ([]
     (make-channel *rabbit-server-petri* "tokengame-log")
     (declare-exchange *rabbit-server-petri* "tokengame-log" "tokengame-log")))

(defn start-framework
  "initialize the required middleware for networks to be executed"
  ([& opts]
     (let [c (apply connect opts)]
       (alter-var-root #'*rabbit-server-petri* (fn [_] c))
       (declare-log-exchange))))

(defmacro with-places-out
  "initialize the required middleware for networks to be executed"
  ([places-out & rest]
     `(binding [*places-out* ~places-out]
        ~@rest)))

(defn defplace
  "Defines a new Petri net place"
  ([name size]
     {:name name
      :size size
      :model-component :place}))

(defn deftransition
  "Defines a new Petri net transition"
  ([name places-in places-out f]
     {:name name
      :places-in places-in
      :places-out places-out
      :function f
      :model-component :transition}))

(defn place-channel-id
  "Creates a new name for a place channel"
  ([place] (str "place-channel" (:name place))))

(defn place-exchange-id
  "Creates a new name for a place exchange"
  ([place] (str "place-exchange" (:name place))))

(defn place-queue-id
  "Creates a new name for a place queue"
  ([place] (str "place-queue-" (:name place))))

(defmulti run
  "Executes one component of the model"
  (fn [component] (:model-component component)))

(defmethod run :place
  ([component]
     (log :info (str "running place: " component))
     (make-channel *rabbit-server-petri* (place-channel-id component))
     (declare-exchange *rabbit-server-petri* (place-channel-id component) (place-exchange-id component))
     (let [queue-id (place-queue-id component)
           queue (make-queue *rabbit-server-petri*
                             (place-channel-id component)
                             queue-id
                             (place-exchange-id component) "token")]
       (assoc component :queue-id queue-id))))

(defn connect-place-out
  ([component]
     (remote-log :info (str "connecting to place: " component))
     (make-channel *rabbit-server-petri* (place-channel-id component))
     (declare-exchange *rabbit-server-petri* (place-channel-id component) (place-exchange-id component))))

(defn bind
  "Retrieves a number of tokens from a place"
  ([place queue acum]
;     (remote-log :info (str "Binding: " place " queue " queue " acum " acum))
     (loop [remaining (:size place)
            to-return []
            source queue]
       (if (= remaining 0)
         (do
;           (remote-log :info (str "Binding " (:name place) " into " acum " with value " to-return))
           (assoc acum (:name place) to-return))
         (let [val-source (first @source)
;               _ (remote-log :info (str "String value from queue: " val-source))
               val (read-json val-source)
               _ (dosync (alter source (fn [q] (rest q))))]
;           (remote-log :info (str "About to dec " remaining " val " val))
           (recur (dec remaining)
                  (conj to-return val)
                  source))))))

;(defn bind
;  "Tries to bind the the tokens in the input places for a transition"
;  ([chn places]
;     (loop [remaining-places places
;            tokens []]
;       (if (empty? remaining-places)
;         ;; All places bound -> ack and return
;         (do (doseq [[t v] tokens]
;               (ack-message *rabbit-server-petri* chn t false))
;             (map (fn [[t v]] v) tokens))
;         (let [next-place (first remaining-places)
;               maybe-token (try-consume *rabbit-server-petri* chn (place-queue-id next-place))]
;           (if (nil? maybe-token)
;             ;; Unbound place -> reject + requeue!
;             (do (doseq [[t v] tokens]
;                   (reject-message *rabbit-server-petri* chn t true))
;                 nil)
;             ;; bound places + 1 -> recur
;             (recur (rest remaining-places)
;                    (conj tokens maybe-token))))))))


(defn fire
  "Puts a token in a set of places"
  ([places token]
;     (remote-log :info (str "*** FIRE: RECEIVED PLACES " places  " TOKEN: " token))
     ;; just one tokengame-place
     (if (and (map? token) (not (nil? (:tokengame-place token))))
       (let [place (first (filter #(= (:name %1) (:tokengame-place token)) places))]
         (if (nil? place)
           (remote-log :error (str "*** Trying to fire unknonw place: " (:tokengame-place token)))
           (recur [place] (:content token))))
       ;; a list of tokengame-places
       (if (and (map? token) (not (nil? (:tokengame-places token))))
         (let [place-names (map (fn [place] (:tokengame-place place)) (:tokengame-places token))
               selected-places (vec (map (fn [name] (first (filter (fn [p] (= (:name p) name)) places))) place-names))]
           (recur selected-places (:content token )))
         ;; send to all places
         (when (not (empty? places))
           (let [place-name (first places)
;                 _ (log :info (str "looking for " place-name " into " places))
                 place (find-place-out place-name)]
             (remote-log :info (str "*** About to fire to place " place " value " token))
             (publish *rabbit-server-petri* (place-channel-id place) (place-exchange-id place) "token" (json-str token))
             (recur (rest places) token)))))))


(defn apply-transition-fn
  ([f args]
     (if (string? f)
       (apply (eval-ns-fn f) args)
       (apply f args))))

(defmethod run :transition
  ([component]
     (let [places-in  (:places-in component)
           places-out (:places-out component)
           places-in (map #(run %1) places-in)
           _ (doseq [place places-out] (connect-place-out place))
           places-in-map (reduce (fn [acum component]
                                   (let [_ (remote-log :info (str "*** Running component: " component))
                                         consumer (ref (make-consumer-queue *rabbit-server-petri* (place-channel-id component) (:queue-id component)))]
                                     (assoc acum component consumer)))
                                 {} places-in)
           places-out (:places-out component)]
       (loop [to-bind places-in
              bound {}]
         (if (empty? to-bind)
           (do (try
                (with-places-out places-out
                  (let [_ (remote-log :info (str  "*** Applying function: " (:function component) " to the bound tokens" ))
                        result (apply-transition-fn (:function component) (apply concat (vals bound)))]
                    (when (not (nil? result))
                      (fire (map (fn [p] (:name p)) *places-out*) result ))))
                (catch Exception ex (remote-log :error (str "*** Error applying custom function: " (.getMessage ex) " \r\n " (vec (.getStackTrace ex))))))
               (recur places-in {}))
           (recur (rest to-bind)
                  (bind (first to-bind) (get places-in-map (first to-bind)) bound)))))))

(defn run-sink
  "Executes a sink receiving duplicated tokens from a place"
  ([place f]
     (make-channel *rabbit-server-petri* (place-channel-id place))
     (declare-exchange *rabbit-server-petri* (place-channel-id place) (place-exchange-id place))
     (let [queue-id (str (place-queue-id place) "-" (java.util.UUID/randomUUID) "-sink")
           _ (make-queue *rabbit-server-petri*
                         (place-channel-id place)
                         queue-id
                         (place-exchange-id place) "token")]
       (f (make-consumer-queue *rabbit-server-petri* (place-channel-id place) queue-id)))))

(defn tail-log
  "Executes a sink receiving duplicated tokens from a place"
  ([]
     (let [queue-id (str  "log-tail-" (java.util.UUID/randomUUID) "-sink")
           _ (make-queue *rabbit-server-petri*
                         "tokengame-log"
                         queue-id
                         "tokengame-log" "log")]
       (loop [queue (make-consumer-queue *rabbit-server-petri* "tokengame-log" queue-id)]
         (println (str (first queue)))
         (recur (rest queue))))))

(defn run-fire
  "Simulates the firing of a transition with a given token"
  ([place token]
     (do
       (connect-place-out place)
       (fire [place] token))))

(defn run-net-locally
  "Runs a net locally, running transitions in futures"
  ([network-description]
     (doseq [t (:transitions network-description)]
       (future (run t)))))

(defn remote-log
  "Logs a message in the log queue"
  ([level msg]
     (let [level-str (if (keyword? level) (name level) level)]
       (publish *rabbit-server-petri* "tokengame-log" "tokengame-log" "log" (str (java.util.Date.) " -> " (localhost) "\r\n" (.toUpperCase level-str) ": " msg)))))
