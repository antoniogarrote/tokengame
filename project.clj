(defproject tokengame "0.0.2-SNAPSHOT"
  :description "Tools for building distributed systems with Clojure"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [matchure "0.9.1"]
                 [com.rabbitmq/amqp-client "2.0.0"]
                 [org.apache.zookeeper/zookeeper "3.3.1"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]]
  :native-path "/usr/local/lib"
  :aot :all)
