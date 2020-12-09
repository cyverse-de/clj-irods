(defproject org.cyverse/clj-irods "0.2.2-SNAPSHOT"
  :description "A Clojure library for interacting with the IRODS data system. Wraps clj-jargon and clj-icat-direct for a nicer interface"
  :url "https://github.com/cyverse-de/clj-irods"
  :license {:name "BSD Standard License"
            :url "http://www.iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[test2junit "1.2.2"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [medley "1.3.0"]
                 [org.cyverse/otel "0.2.0"]
                 [org.cyverse/clojure-commons "2.8.3"]
                 [org.cyverse/clj-icat-direct "2.9.1"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [org.cyverse/clj-jargon "2.8.12"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [slingshot "0.12.2"]])
