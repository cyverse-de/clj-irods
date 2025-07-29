(defproject org.cyverse/clj-irods "0.3.9-SNAPSHOT"
  :description "A Clojure library for interacting with the IRODS data system. Wraps clj-jargon and clj-icat-direct for a nicer interface"
  :url "https://github.com/cyverse-de/clj-irods"
  :license {:name "BSD Standard License"
            :url "https://cyverse.org/license"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[jonase/eastwood "1.4.3"]
            [lein-ancient "0.7.0"]
            [test2junit "1.4.4"]]
  :profiles {:repl {:dependencies [[cheshire "5.13.0"]]
                    :source-paths ["repl"]}}
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [medley "1.4.0"]
                 [org.cyverse/clojure-commons "3.0.9"]
                 [org.cyverse/clj-icat-direct "2.9.7"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [org.cyverse/clj-jargon "3.1.3-SNAPSHOT"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [slingshot "0.12.2"]])
