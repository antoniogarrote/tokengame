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
(defonce *node-links-znode* "/tokengame/links")
(defonce *pid* nil)
(defonce *mbox* nil)
(def *process-table* (ref {}))
(def *process-count* (ref 0))
(def *rpc-table* (ref {}))
(def *rpc-count* (ref 0))
(def *links-table* (ref {}))
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

(defn protocol-link-new
  ([from to tx-name]
     {:type :msg
      :topic :link-new
      :from from
      :to   to
      :content {:tx-name tx-name}}))

(defn protocol-link-broken
  ([from to cause]
     {:type  :signal
      :topic :link-broken
      :from from
      :to   to
      :content {:cause cause}}))

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
        (when (nil? (zk/exists? *node-links-znode*))
          (zk/create *node-links-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-links-znode*))
          (zk/create *node-processes-znode* "/" {:world [:all]} :persistent)))))

(defn json-encode
  "Default encoding of messages"
  ([msg]
     (json-str msg)))

(defn json-decode
  "Default decoding of messages"
  ([msg]
     (read-json msg)))

(defn java-encode
  ([obj] (let [bos (java.io.ByteArrayOutputStream.)
               oos (java.io.ObjectOutputStream. bos)]
           (.writeObject oos obj)
           (.close oos)
           (String. (.toByteArray bos)))))

(defn java-decode
  ([string] (let [bis (java.io.ByteArrayInputStream. (.getBytes string))
                  ois (java.io.ObjectInputStream. bis)
                  obj (.readObject ois)]
              (.close ois)
              obj)))

(defonce default-encode json-encode)
(defonce default-decode json-decode)

(defn zk-process-path
  ([pid] (str *node-processes-znode* "/" pid)))

(defn zk-link-tx-path
  ([tx-name] (str *node-links-znode* "/" tx-name)))

(defn add-link
  ([tx-name self-pid remote-pid]
;     (println (str "ADDING LINK from " self-pid " to " remote-pid ))
     (dosync (alter *links-table* (fn [table]
                                    (let [old-list (get table self-pid)
                                          old-list (if (nil? old-list) [] old-list)]
                                      (assoc table self-pid (conj old-list remote-pid))))))))

(defn remove-link
  ([self-pid remote-pid]
     (dosync (alter *links-table* (fn [table]
                                    (let [old-list (get table self-pid)]
                                      (if (nil? old-list) table
                                          (let [new-list (filter #(not (= %1 remote-pid)) old-list)]
                                            (assoc table self-pid new-list)))))))))

(defn remove-links
  ([self-pid]
     (dosync (alter *links-table* (fn [table]
                                    (let [old-list (get table self-pid)]
                                      (if (nil? old-list) table
                                          (dissoc table self-pid))))))))

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
;       (println (str "invoking " function " with " args " from " from " should return? " should-return))
       (try
        (let [f (eval-fn function)
              result (apply f args)]
;          (println (str "Success RPC, result " result))
          (when should-return
            (let [node (pid-to-node-id from)
                  resp (protocol-answer result internal-id)]
              (rabbit/declare-exchange *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node))
              (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode resp)))))
        (catch Exception ex
          (do
            (log :error (str "Error invoking RPC call"))))))))


(declare exists-pid?)
(defn handle-link-request
  ([msg] (let [from (:from msg)
               to (:to msg)
               tx-name (:tx-name (:content msg))]
;           (println (str "handling link request -> " msg))
           (when (exists-pid? to)
             ;; @todo add timeout here
;             (println (str "commiting " tx-name " " to))
             (let [result (zk/commit (zk-link-tx-path tx-name) to)]
               (when (= result "commit")
                 (add-link tx-name to from)))))))

(declare pid-to-mbox)
(declare rpc-id-to-promise)
(declare remove-rpc-promise)
(defn dispatch-signal
  ([msg]
     (condp = (keyword (:topic msg))
       :link-broken (if-let [mbox (pid-to-mbox (:to msg))]
                      (do
                        (remove-link (:to msg) (:from msg))
                        (.put mbox {:signal :link-broken
                                    :cause (:cause (:content msg))}))))))

(defn dispatch-msg
  ([msg]
;     (println (str "Dispatching msg " (keyword (:topic msg))))
     (condp = (keyword (:topic msg))
       :process (if-let [mbox (pid-to-mbox (:to msg))]
                  (.put mbox (:content msg)))
       :link-new (future (handle-link-request msg))
       :rpc     (future (process-rpc msg))
       :rpc-response (future (try
                              (let [prom (rpc-id-to-promise (:internal-id (:content msg)))
                                    val (:value (:content msg))]
                                (deliver prom val)
;                                (println (str "removing rpc promise for id " (:internal-id (:content msg)) " and  class " (class (:internal-id (:content msg)))))
                                (remove-rpc-promise (:internal-id (:content msg))))
                              (catch Exception ex (log :error "Error processing rpc-response")))))))

(defn node-dispatcher-thread
  "The main thread receiving events and messages"
  ([name id queue]
     (loop [should-continue true
            queue queue]
       (let [msg (.take queue)]
;         (println (str "READ FROM QUEUE " msg))
         (try
          (do
            (let [msg (default-decode msg)]
              (condp = (keyword (:type msg))
                :msg (dispatch-msg msg)
                :signal (dispatch-signal msg)
                (log :error (str "*** " name " , " (java.util.Date.) " uknown message type for : " msg)))))
          (catch Exception ex (log :error (str "***  " name " , " (java.util.Date.) " error processing message : " msg " --> " (.getMessage ex)))))
         (recur true queue)))))


(declare bootstrap-node)
(defn- bootstrap-from-file
  ([file-path]
     (let [config (eval (read-string (slurp file-path)))]
       (bootstrap-node (:node-name config) (:rabbit-options config) (:zookeeper-options config)))))

(defn bootstrap-node
  "Adds a new node to the distributed application"
  ([file-path] (bootstrap-from-file file-path))
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

(defn- exists-pid?
  ([pid] (not (nil? (get @*process-table* (pid-to-process-number pid))))))

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

(declare send!)
(defn notify-links
  ([pid cause]
     (if-let [pids (get @*links-table* pid)]
       (doseq [linked pids]
         (send! linked (protocol-link-broken pid linked cause))))))

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
           mbox (pid-to-mbox pid)
           f (if (string? f) (eval-fn f) f)]
       (future
        (binding [*pid* pid
                  *mbox* mbox]
          (try
           (let [result (f)]
             (clean-process *pid*))
           (catch Exception ex
             (log :error (str "*** process " pid " died with message : " (.getMessage ex)))
             (notify-links *pid* (str (class ex) ":" (.getMessage ex)))
             (remove-links *pid*)
             (clean-process *pid*)))))
         pid)))

(defn spawn-in-repl
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
       (let [msg (if (and (= (map? msg))
                          (= :signal (keyword (:type msg))))
                   msg
                   (protocol-process-msg (self) pid msg))]
         (rabbit/declare-exchange *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node))
         (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode msg)))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn admin-send
  ([node pid msg]
     (if (zk/exists? (zk-process-path pid))
       (do
         (rabbit/declare-exchange *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node))
         (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode msg)))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn send!
  "Sends a message to a local/remote process"
  ([pid msg]
     (let [node (pid-to-node-id pid)]
       (if (= node @*node-id*)
         (if (and (= (map? msg))
                  (= :signal (keyword (:type msg))))
           (dispatch-signal msg)
           (let [mbox (pid-to-mbox pid)]
             (.put mbox msg)))
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
  "Executes a blocking RPC call"
  ([node function args]
     (let [rpc-id (next-rpc-id)
           prom (register-rpc-promise rpc-id)
           msg (protocol-rpc (self) function args true rpc-id)]
       (rabbit/publish *rabbit-server* (node-channel-id @*node-id*) (node-exchange-id node) "msg" (default-encode msg))
       @prom)))

(defn link
  "Links this process with the process identified by the provided PID. Links are bidirectional"
  ([pid]
     (let [pid-number  (apply + (map #(int %1) (vec pid)))
           self-number (apply + (map #(int %1) (vec (self))))
           tx-name (if (< pid-number self-number) (str pid "-" (self)) (str (self) "-" pid))
           ;; we create a transaction for committing on the link
           _2pc (zk/make-2-phase-commit (zk-link-tx-path tx-name) [pid (self)])]
       ;; we send the link request
;       (println (str "about to send request " pid " -> " (protocol-link-new (self) pid tx-name)))
       (admin-send (pid-to-node-id pid) pid (protocol-link-new (self) pid tx-name))
       ;; we commit and wait for a result
       ;; @todo add timeout!
;       (println (str "about to block for commit"))
       (let [result (zk/commit (zk-link-tx-path tx-name) (self))]
;         (println (str "get response " result))
         (if (= result "commit")
           (add-link tx-name (self) pid)
           (throw (Exception. (str "Error linking processes " (self) " - " pid))))))))
