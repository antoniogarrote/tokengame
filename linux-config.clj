;; Default configuration for a node

{:node-name "linux"
 :rabbit-options [:host "172.21.1.238"]
 :zookeeper-options ["172.21.1.238:2181" {:timeout 3000}]}
