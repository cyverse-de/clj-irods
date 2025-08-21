(defproject org.cyverse/clj-irods "0.4.0-SNAPSHOT"
  :description "A Clojure library for interacting with the IRODS data system. Wraps clj-jargon and clj-icat-direct for a nicer interface"
  :url "https://github.com/cyverse-de/clj-irods"
  :license {:name "BSD Standard License"
            :url "https://cyverse.org/license"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[jonase/eastwood "1.4.3"]
            [lein-ancient "0.7.0"]
            [test2junit "1.4.4"]]
  :profiles {:repl {:dependencies [[cheshire "6.0.0"]]
                    :source-paths ["repl"]}}
  :dependencies [[org.clojure/clojure "1.12.1"]
                 [medley "1.4.0"]
                 [org.cyverse/clojure-commons "3.0.11"]
                 [org.cyverse/clj-icat-direct "2.9.7"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [org.cyverse/clj-jargon "3.1.3"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [slingshot "0.12.2"]])
