(defproject tokengame "0.0.2-SNAPSHOT"
  :description "Tools for building distributed systems with Clojure"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [javax.jms/jms "1.1"]
                 [com.sun.jmx/jmxri "1.2.1"]
                 [matchure "0.9.1"]
                 [com.rabbitmq/amqp-client "2.0.0"]
                 [org.apache.zookeeper/zookeeper "3.3.1"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]]
  :aot :all)
