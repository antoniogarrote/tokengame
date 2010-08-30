(ns tokengame.zookeeper
  (:import [org.apache.zookeeper
            ZooKeeper
            Watcher]
           [org.apache.zookeeper.data
            Id
            ACL]))

(defonce *zk* nil)

(defn- watcher
  "Creates a new watcher with the provided function"
  ([f] (proxy [Watcher] []
         (process [event] (f event)))))

(defn- stat-callback
  "Creates a new callback"
  ([f] (proxy [org.apache.zookeeper.AsyncCallback$StatCallback] []
         (processResult [rc path ctx stat]
                        (f rc path stat)))))

(defn- data-callback
  "Creates a new callback"
  ([f] (proxy [org.apache.zookeeper.AsyncCallback$DataCallback] []
         (processResult [rc path ctx data stat]
                        (f rc path data stat)))))

(defn connect
  "Connects to a zookeeper cluster"
  ([servers opts]
     (let [servers-str (if (string? servers) servers (apply str servers))
           session-timeout (:timeout opts)
           watcher-fn (:watcher opts)
           session-id (:id opts)
           password (:password opts)
           connection (atom nil)]
       (let [zk (if (nil? password)
                  (ZooKeeper. servers-str session-timeout (watcher watcher-fn))
                  (ZooKeeper. servers-str session-timeout (watcher watcher-fn (long session-id) (.getBytes password))))]
         (dosync (swap! connection (fn [_] zk)))
         (alter-var-root #'*zk* (fn [_] {:servers servers
                                         :session-timeout session-timeout
                                         :session-id session-id
                                         :password password
                                         :connection @connection}))
         *zk*))))

(defn make
  "Connects to a zookeeper cluster and returns a connection object"
  ([servers opts]
     (let [servers-str (if (string? servers) servers (apply str servers))
           session-timeout (:timeout opts)
           watcher-fn (:watcher opts)
           session-id (:id opts)
           password (:password opts)
           connection (atom nil)]
       (let [zk (if (nil? password)
                  (ZooKeeper. servers-str session-timeout (watcher watcher-fn))
                  (ZooKeeper. servers-str session-timeout (watcher watcher-fn (long session-id) (.getBytes password))))]
         (dosync (swap! connection (fn [_] zk)))
         {:servers servers
          :session-timeout session-timeout
          :session-id session-id
          :password password
          :connection @connection}))))

(defn state
  "Get the state of the connection"
  ([] (.getState (:connection *zk*))))

(defn close
  ([]
     (do (.close (:connection *zk*))
         (alter-var-root #'*zk* {}))))

(defn- permission-sym
  ([p] (condp = p
         :all    org.apache.zookeeper.ZooDefs$Perms/ALL
         :admin  org.apache.zookeeper.ZooDefs$Perms/ADMIN
         :create org.apache.zookeeper.ZooDefs$Perms/CREATE
         :delete org.apache.zookeeper.ZooDefs$Perms/DELETE
         :read   org.apache.zookeeper.ZooDefs$Perms/READ
         :write  org.apache.zookeeper.ZooDefs$Perms/WRITE
         (throw (Exception. (str "Unknown zookeeper permission: " p))))))


(defn- world-scheme
  ([] (let [id (Id.)]
        (.setScheme id "world")
        (.setId id "anyone")
        id)))

(defn- auth-scheme
  ([] org.apache.zookeeper.ZooDefs$Ids/AUTH_IDS))

(defn- digest-scheme
  ([username-password] (let [id (Id.)]
        (.setScheme id "digest")
        (.setId id username-password)
        id)))

(defn- ip-scheme
  ([ip] (let [id (Id.)]
        (.setScheme id "ip")
        (.setId id ip)
        id)))

(defn- process-acl-map-pre
  ([acl-map]
     (reduce (fn [ac [k perms]] (assoc ac k (map #(permission-sym %1) perms))) {} acl-map)))

(defn- make-acl
  ([scheme perms]
     (println (str "making acl for " scheme " -> " perms " ? " (coll? scheme) " ... " (map (fn [p] (ACL. p (world-scheme))) perms)))
     (if (coll? scheme)
       (let [[s d] scheme]
         (condp = s
           :digest (map (fn [p] (ACL. p (digest-scheme d))) perms)
           :ip     (map (fn [p] (ACL. p (ip-scheme d))) perms)
           (throw (Exception. "Uknown ACL scheme: " s))))
       (condp = scheme
         :world (map (fn [p] (ACL. p (world-scheme))) perms)
         :auth (map (fn [p] (ACL. p (auth-scheme))) perms)
         (throw (Exception. "Uknown ACL scheme: " scheme))))))

(defn- process-acl-map
  ([acl-map]
     (let [acl-map (process-acl-map-pre acl-map)
           acl-map (reduce (fn [ac [scheme perms]]
                             (let [next-acl-list (make-acl scheme perms)
                                   _ (println (str "ACL LIST " next-acl-list))]
                               (concat ac next-acl-list))) [] acl-map)]
       (vec acl-map))))

(defn- process-create-mode
  ([create-mode]
     (condp = create-mode
       :ephemeral org.apache.zookeeper.CreateMode/EPHEMERAL
       :ephemeral-sequential org.apache.zookeeper.CreateMode/EPHEMERAL_SEQUENTIAL
       :persistent org.apache.zookeeper.CreateMode/PERSISTENT
       :persistent-sequential org.apache.zookeeper.CreateMode/PERSISTENT_SEQUENTIAL
       (throw (Exception. (str "Unknown create mode " create-mode))))))

(defn- stat-to-map
  "Creates a map with the data in a stat object"
  ([stat] {:aversion (.getAversion stat)
           :ctime (.getCtime stat)
           :cversion (.getCversion stat)
           :czxid (.getCzxid stat)
           :data-length (.getDataLength stat)
           :ephemeral-owner (.getEphemeralOwner stat)
           :mtime (.getMtime stat)
           :mzxid (.getMzxid stat)
           :num-children (.getNumChildren stat)
           :pzxid (.getPzxid stat)
           :version (.getVersion stat)}))

(defn- map-to-stat
  "Creates a new stat object from a stat map"
  ([m]
     (org.apache.zookeeper.data.Stat.
      (:czxid m)
      (:mzxid m)
      (:ctime m)
      (:mtime m)
      (:version m)
      (:cversion m)
      (:aversion m)
      (:ephemeral-owner m)
      (:data-length m)
      (:num-children m)
      (:pzxid m))))

(defn create
  "Creates a new znode in a zookeeper server"
  ([path data acl-map create-mode]
     (let [data (if (string? data) (.getBytes data) data)]
       (.create (:connection *zk*) path data (process-acl-map acl-map) (process-create-mode create-mode)))))

(defn exists
  "Checks the stats for the znode"
  ([path & opts]
     (let [should-watch-or-fn (if (empty? opts) false (first opts))]
       (if (fn? should-watch-or-fn)
         (let [f should-watch-or-fn]
           (.exists (:connection *zk*) path (watcher f)))
         (let [result (.exists (:connection *zk*) path should-watch-or-fn)]
           (if (nil? result) result (stat-to-map result)))))))


(defn get-data
  "Retrieve the data from the provided znode path"
  ([path]
     (let [to-return (promise)]
       (.getData (:connection *zk*) path false (data-callback (fn [rc path data stat]
                                                                (deliver to-return [data (stat-to-map stat)]))) {})
       @to-return))

  ([path stat]
     (.getData (:connection *zk*) path false (map-to-stat stat)))

  ([path stat f]
     (if (fn? f)
       (.getData (:connection *zk*) path (watcher f) (map-to-stat stat))
       (let [should-watch f]
         (.getData (:connection *zk*) path should-watch (map-to-stat stat))))))

(defn- maybe-version
  ([mv] (if (map? mv) (:version mv) mv)))

(defn set-data
  "Sets the value of a znode"
  ([path data version]
     (let [data (if (string? data) (.getBytes data) data)]
       (stat-to-map (.setData (:connection *zk*) path data (maybe-version version))))))

(defn delete
  "Deletes a znode"
  ([path version]
     (.delete (:connection *zk*) path (maybe-version version))))


(defn get-children
  "Returns all the children from a znode"
  ([path]
     (vec (.getChildren (:connection *zk*) path false)))
  ([path f]
     (vec (.getChildren (:connection *zk*) path (watcher f)))))

(defmacro with-zookeeper
  ([zk & body]
     `(binding [*zk* ~zk]
        ~@body)))