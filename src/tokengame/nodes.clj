(ns tokengame.nodes
  (:require [tokengame.rabbit :as rabbit]
            [tokengame.zookeeper :as zk]
            [tokengame.utils])
  (:use [tokengame.utils]
        [clojure.contrib.logging :only [log]]
        [clojure.contrib.json]))

(defonce *rabbit-server* nil)
(defonce *node-app-znode* "/tokengame")
(defonce *node-nodes-znode* "/tokengame/nodes")
(defonce *node-processes-znode* "/tokengame/processes")
(defonce *node-names-znode* "/tokengame/names")
(defonce *pid* nil)
(defonce *mbox* nil)
(def *process-table* (ref {}))
(def *process-count* (ref 0))
(def *rpc-table* (ref {}))
(def *rpc-count* (ref 0))
(def *node-id* (ref nil))

;; protocol

(defn protocol-process-msg
  ([from to msg]
     {:type :msg
      :topic :process
      :to   to
      :from from
      :content msg}))

(defn protocol-rpc
  ([from function args should-return internal-id]
     {:type :msg
      :topic :rpc
      :from from
      :content {:function      function
                :args          args
                :should-return should-return
                :internal-id    internal-id}}))

(defn protocol-answer
  ([value internal-id]
     {:type :msg
      :topic :rpc-response
      :content {:value value
                :internal-id internal-id}}))

(defn admin-msg
  ([command args from to]
     {:type  :msg
      :topic :admin
      :from  from
      :to    to
      :content { :command command
                 :args    args}}))



;; utility functions

(defn- node-channel-id
  ([node-id] (str "node-channel-" node-id)))

(defn- node-exchange-id
  ([node-id] (str "node-exchange-" node-id)))

(defn- node-queue-id
  ([node-id] (str "node-queue-" node-id)))


(defn check-default-znodes
  "Creates the default znodes for the distributed application to run"
  ([] (do
        (when (nil? (zk/exists? *node-app-znode*))
          (zk/create *node-app-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-nodes-znode*))
          (zk/create *node-nodes-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-names-znode*))
          (zk/create *node-names-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-processes-znode*))
          (zk/create *node-processes-znode* "/" {:world [:all]} :persistent)))))

(defn default-encode
  "Default encoding of messages"
  ([msg]
     (json-str msg)))

(defn default-decode
  "Default decoding of messages"
  ([msg]
     (read-json msg)))

(defn zk-process-path
  ([pid] (str *node-processes-znode* "/" pid)))

;; core functions for nodes

(declare pid-to-node-id)
(defn process-rpc
  "Process an incoming RPC request"
  ([msg]
     (let [from (get msg :from)
           content (get msg :content)
           function (get content :function)
           args     (get content :args)
           should-return (get content :should-return)
           internal-id (get content :internal-id)]
       (println (str "invoking " function " with " args " from " from " should return? " should-return))
       (try
        (let [f (eval-ns-fn function)
              result (apply f args)]
          (println (str "Success RPC, result " result))
          (when should-return
            (let [node (pid-to-node-id from)
                  resp (protocol-answer result internal-id)]
              (rabbit/declare-exchange *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node))
              (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode resp)))))
        (catch Exception ex
          (do
            (log :error (str "Error invoking RPC call"))))))))

(declare pid-to-mbox)
(declare rpc-id-to-promise)
(declare remove-rpc-promise)
(defn dispatch-msg
  ([msg]
     (condp = (keyword (:topic msg))
       :process (let [mbox (pid-to-mbox (:to msg))]
                  (.put mbox (:content msg)))
       :rpc     (future (process-rpc msg))
       :rpc-response (future (try
                              (let [prom (rpc-id-to-promise (:internal-id (:content msg)))
                                    val (:value (:content msg))]
                                (deliver prom val)
                                (remove-rpc-promise (:internal-id (:content msg))))
                              (catch Exception ex (log :error "Error processing rpc-response")))))))

(defn node-dispatcher-thread
  "The main thread receiving events and messages"
  ([name id queue]
     (loop [should-continue true
            queue queue]
       (let [msg (.take queue)]
         (try
          (do
            (let [msg (default-decode msg)]
              (condp = (keyword (:type msg))
                :msg (dispatch-msg msg)
                (log :error (str "*** " name " , " (java.util.Date.) " uknown message type for : " msg)))))
          (catch Exception ex (log :error (str "***  " name " , " (java.util.Date.) " error processing message : " msg " --> " (.getMessage ex)))))
         (recur true queue)))))


(defn bootstrap-node
  "Adds a new node to the distributed application"
  ([name rabbit-args zookeeper-args]
     (let [id (random-uuid)
           ;; connecting to RabbitMQ
           rc (apply rabbit/connect rabbit-args)]
       ;; store node configuration
       (dosync (alter *node-id* (fn [_] id)))
       (alter-var-root #'*rabbit-server* (fn [_] rc))
       ;; connecting to ZooKeeper
       (apply zk/connect zookeeper-args)
       ;; declare queuing components
       (rabbit/make-channel *rabbit-server* (node-channel-id id))
       (rabbit/declare-exchange *rabbit-server* (node-channel-id id) (node-exchange-id id))
       (rabbit/make-queue *rabbit-server* (node-channel-id id) (node-queue-id id) (node-exchange-id id) "msg")
       ;; check application standard znodes
       (check-default-znodes)
       ;; connect messages
       (let [dispatcher-queue (java.util.concurrent.LinkedBlockingQueue.)]
         (rabbit/make-consumer *rabbit-server* (node-channel-id id) (node-queue-id id)
                               (fn [msg] (.put dispatcher-queue msg)))
         (zk/watch-group *node-nodes-znode* (fn [evt] (.put dispatcher-queue evt)))
         ;; starting main thread
         (.start (Thread. (fn [] (node-dispatcher-thread name id  dispatcher-queue))))
         ;; register zookeeper group
         (zk/join-group *node-nodes-znode* name id)))))

;; library functions

(defn nodes
  "Returns all the available nodes and their identifiers"
  ([] (let [children (zk/get-children *node-nodes-znode*)]
        (reduce (fn [m c] (let [[data stats] (zk/get-data (str *node-nodes-znode* "/" c))]
                            (assoc m c (String. data))))
                {}
                children))))

(defn- next-process-id
  ([] (let [rpid (dosync (alter *process-count* (fn [old] (inc old))))
            lpid (deref *node-id*)]
        (str lpid "." rpid))))

(defn- next-rpc-id
  ([] (let [rpc (dosync (alter *rpc-count* (fn [old] (inc old))))]
        rpc)))

(defn pid-to-process-number
  ([pid] (last (vec (.split pid "\\.")))))

(defn pid-to-node-id
  ([pid] (first (vec (.split pid "\\.")))))

(defn- register-local-mailbox
  ([pid]
     (let [process-number (pid-to-process-number pid)
           q (java.util.concurrent.LinkedBlockingQueue.)]
       (dosync (alter *process-table* (fn [table] (assoc table process-number {:mbox q
                                                                               :dictionary {}}))))
       q)))

(defn- register-rpc-promise
  ([rpc-id]
     (let [p (promise)]
       (dosync (alter *rpc-table* (fn [table] (assoc table rpc-id p))))
       p)))

(defn- rpc-id-to-promise
  ([rpc-id]
     (get @*rpc-table* rpc-id)))

(defn- remove-rpc-promise
  ([rpc-id]
     (alter @*rpc-table* (fn [table] (dissoc table rpc-id)))))

(defn pid-to-mbox
  ([pid] (let [rpid (pid-to-process-number pid)]
           (:mbox (get @*process-table* rpid)))))

(defn dictionary-write
  ([pid key value]
     (let [process-number (pid-to-process-number pid)
           dictionary (get @*process-table* process-number)
           mbox (pid-to-mbox pid)]
       (dosync (alter *process-table* (fn [table] (assoc table process-number {:mbox mbox
                                                                               :dictionary (assoc dictionary key value)})))))))
(defn dictionary-get
  ([pid key]
     (let [process-number (pid-to-process-number pid)
           dictionary (get @*process-table* process-number)]
       (get dictionary key))))

(defn clean-process
  ([pid]
     (let [version (:version (second (zk/get-data (zk-process-path pid))))
           registered (dictionary-get pid :registered-name)]
       (zk/delete (zk-process-path pid) version)
       (when (not (nil? registered))
         (let [version (:version (second (zk/get-data (str *node-names-znode* "/" registered))))]
           (zk/delete (str *node-names-znode* "/" registered)) version))
       (dosync (alter *process-table* (fn [table] (dissoc table (pid-to-process-number pid))))))))

(defn spawn
  "Creates a new local process"
  ([]
     (let [pid (next-process-id)
           queue (register-local-mailbox pid)]
       (zk/create (zk-process-path pid) {:world [:all]} :ephemeral)
       (zk/set-data (zk-process-path pid) "running" 0)
       pid))
  ([f]
     (let [pid (spawn)
           mbox (pid-to-mbox pid)]
       (future
        (binding [*pid* pid
                  *mbox* mbox]
          (try
           (let [result (f)]
             (clean-process *pid*))
           (catch Exception ex
             (log :error (str "*** process " pid " died with message : " (.getMessage ex)))
             (clean-process *pid*)))))
         pid)))

(defn spawn-in-shell
  "Creates a new process attached to the running shell"
  ([] (let [pid (spawn)]
        (alter-var-root #'*pid* (fn [_] pid))
        (alter-var-root #'*mbox* (fn [_] (pid-to-mbox pid)))
        pid)))

(defn self
  "Returns the pid of the current process"
  ([] *pid*))


(defn remote-send
  ([node pid msg]
     (if (zk/exists? (zk-process-path pid))
       (do
         (rabbit/declare-exchange *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node))
         (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode (protocol-process-msg (self) pid msg))))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn send!
  "Sends a message to a local/remote process"
  ([pid msg]
     (let [node (pid-to-node-id pid)]
       (if (= node @*node-id*)
         (let [mbox (pid-to-mbox pid)]
           (.put mbox msg))
         (remote-send node pid msg)))))

(defn receive
  "Blocks until a new message has been received"
  ([] (let [msg (.take *mbox*)]
        msg)))

(defn register-name
  "Associates a name that can be retrieved from any node to a PID"
  [name pid]
  (if (zk/exists? (str *node-names-znode* "/" name))
    (throw (Exception. (str "Non existent remote process " pid)))
    (do (zk/create (str *node-names-znode* "/" name) pid {:world [:all]} :ephemeral)
        (dictionary-write pid :registered-name name)
        :ok)))

(defn registered-names
  "The list of globally registered names"
  ([]
     (let [children (zk/get-children *node-names-znode*)]
       (reduce (fn [m c] (let [[data stats] (zk/get-data (str *node-names-znode* "/" c))]
                           (assoc m c (String. data))))
               {}
               children))))

(defn resolve-name
  "Wraps a globally registered name"
  ([name]
     (if (zk/exists? (str *node-names-znode* "/" name))
       (String. (first (zk/get-data (str *node-names-znode* "/" name)))))))

(defn resolve-node-name
  "Returns the identifier for a provided node name"
  ([node-name]
     (let [stat (zk/exists? (str *node-nodes-znode* "/" node-name))]
       (if stat
         (let [[data stats] (zk/get-data (str *node-nodes-znode* "/" node-name))]
           (String. data))
         (throw (Exception. (str "Unknown node " node-name)))))))

(defn rpc-call
  "Executes a non blocking RPC call"
  ([node function args]
     (let [msg (protocol-rpc (self) function args false 0)]
       (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode msg)))))

(defn rpc-blocking-call
  "Executes a non blocking RPC call"
  ([node function args]
     (let [rpc-id (next-rpc-id)
           prom (register-rpc-promise rpc-id)
           msg (protocol-rpc (self) function args true rpc-id)]
       (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode msg))
       @prom)))