(defproject clj-irods "0.1.0-SNAPSHOT"
  :description "A Clojure library for interacting with the IRODS data system. Wraps clj-jargon and clj-icat-direct for a nicer interface"
  :url "https://github.com/cyverse-de/clj-irods"
  :license {:name "BSD Standard License"
            :url "http://www.iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.cyverse/clojure-commons "2.8.3"]
                 [org.cyverse/clj-icat-direct "2.8.8-SNAPSHOT"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [org.cyverse/clj-jargon "2.8.11-SNAPSHOT"
                   :exclusions [[org.slf4j/slf4j-log4j12]
                                [log4j]]]
                 [slingshot "0.12.2"]])
