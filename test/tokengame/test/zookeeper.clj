(ns tokengame.test.zookeeper
  (:use [tokengame.zookeeper] :reload-all)
  (:use [clojure.test]))

(deftest should-create-watcher
  (let [flag (ref false)
        w (watcher (fn [e] (dosync (alter flag (fn [old] true)) )))]
    (.process w (org.apache.zookeeper.WatchedEvent. org.apache.zookeeper.Watcher$Event$EventType/None org.apache.zookeeper.Watcher$Event$KeeperState/Unknown "test"))
    (is @flag)))

(deftest should-get-the-code-for-a-permission
  (is (= org.apache.zookeeper.ZooDefs$Perms/ALL    (permission-sym :all)))
  (is (= org.apache.zookeeper.ZooDefs$Perms/ADMIN  (permission-sym :admin)))
  (is (= org.apache.zookeeper.ZooDefs$Perms/CREATE (permission-sym :create)))
  (is (= org.apache.zookeeper.ZooDefs$Perms/DELETE (permission-sym :delete)))
  (is (= org.apache.zookeeper.ZooDefs$Perms/READ   (permission-sym :read)))
  (is (= org.apache.zookeeper.ZooDefs$Perms/WRITE  (permission-sym :write))))
