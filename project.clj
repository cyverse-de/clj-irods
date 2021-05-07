(defproject org.cyverse/clj-irods "0.3.0-SNAPSHOT"
  :description "A Clojure library for interacting with the IRODS data system. Wraps clj-jargon and clj-icat-direct for a nicer interface"
  :url "https://github.com/cyverse-de/clj-irods"
  :license {:name "BSD Standard License"
            :url "http://www.iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[jonase/eastwood "0.3.14"]
            [test2junit "1.2.2"]]
  :profiles {:repl {:dependencies [[cheshire "5.10.0"]]
                    :source-paths ["repl"]}}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [medley "1.3.0"]
                 [org.cyverse/otel "0.2.4"]
                 [org.cyverse/clojure-commons "3.0.6"]
                 [org.cyverse/clj-icat-direct "2.9.3"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [org.cyverse/clj-jargon "3.0.0"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [slingshot "0.12.2"]])
