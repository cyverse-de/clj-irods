(defproject org.cyverse/clj-irods "0.4.3-SNAPSHOT"
  :description "A Clojure library for interacting with the IRODS data system. Wraps clj-jargon and clj-icat-direct for a nicer interface"
  :url "https://github.com/cyverse-de/clj-irods"
  :license {:name "BSD Standard License"
            :url "https://cyverse.org/license"}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :plugins [[jonase/eastwood "1.4.3"]
            [lein-ancient "1.0.0"]
            [test2junit "1.4.4"]]
  :profiles {:repl {:dependencies [[cheshire "6.2.0"]]
                    :source-paths ["repl"]}}
  ;; Fail the build on a new dependency conflict rather than printing a
  ;; warning nobody reads.
  :pedantic? :abort
  ;; Records versions Leiningen already resolves, read off the resolved
  ;; classpath rather than copied from lein's "Consider using these
  ;; :managed-dependencies" hint -- that hint names the version that LOST the
  ;; conflict, so pasting it would be a silent upgrade.
  ;;
  ;; The jackson-* entries deserve a note. This repo sits where two constraints
  ;; meet: clj-jargon holds databind/cbor/smile at the 2.14.1 that jargon-core
  ;; 4.3.7.0-RELEASE brings (pinned :upgrade false for iRODS), while
  ;; clojure-commons brings cheshire 6.2.0, which needs a much newer
  ;; jackson-core. The family is therefore deliberately NOT uniform, and that
  ;; matches what main already resolved. Verified by exercising cheshire's
  ;; json, cbor and smile paths against exactly these versions.
  ;;
  ;; The load-bearing constraint is the FLOOR on jackson-core: cheshire 6.2.0
  ;; throws at runtime against jackson-core 2.14.1. Do not "tidy" this by
  ;; dropping core to match the rest. Unifying the family means moving jargon.
  :managed-dependencies [[cheshire "6.2.0"]
                         [com.fasterxml.jackson.core/jackson-annotations "2.14.1"]
                         [com.fasterxml.jackson.core/jackson-core "2.21.1"]
                         [com.fasterxml.jackson.core/jackson-databind "2.14.1"]
                         [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor "2.14.1"]
                         [com.fasterxml.jackson.dataformat/jackson-dataformat-smile "2.14.1"]
                         [commons-codec "1.16.1"]
                         [org.apache.commons/commons-compress "1.8"]
                         [prismatic/schema "1.1.12"]
                         [ring/ring-codec "1.1.0"]
                         [ring/ring-core "1.6.3"]]
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [dev.weavejester/medley "1.10.0"]
                 [org.cyverse/clojure-commons "3.0.13"
                  :exclusions [[medley]]]
                 [org.cyverse/clj-icat-direct "2.9.8"
                  :exclusions [[org.slf4j/slf4j-log4j12]
                               [log4j]]]
                 [org.cyverse/clj-jargon "3.1.6"
                  :exclusions [[org.slf4j/slf4j-log4j12]
                               [log4j]]]
                 [slingshot "0.12.2"]])
