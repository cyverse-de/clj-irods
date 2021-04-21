(ns clj-irods.repl-utils
  (:require [cheshire.core :as json]
            [clj-icat-direct.icat :as icat]
            [clj-jargon.init :as jargon-init]
            [clojure.java.io :as io]))

(def rods-conf-dir (io/file (System/getProperty "user.home") ".irods"))
(def prod-icat-config (io/file rods-conf-dir ".prod-db.json"))
(def prod-jargon-config (io/file rods-conf-dir ".prod-jargon.json"))
(def qa-icat-config (io/file rods-conf-dir ".qa-db.json"))
(def qa-jargon-config (io/file rods-conf-dir ".qa-jargon.json"))

(defn- load-config [path]
  (json/decode-stream (io/reader path) true))

(defn- init-icat [path]
  (let [{:keys [host port user password]} (load-config path)]
    (icat/setup-icat (icat/icat-db-spec host user password :port port))))

(defn init-jargon [path]
  (let [{:keys [host zone port user password home resource max-retries retry-sleep use-trash]} (load-config path)]
    (jargon-init/init host port user password home zone resource
                      :max-retries max-retries
                      :retry-sleep retry-sleep
                      :use-trash use-trash)))

(defn- init [icat-config-path rods-config-path]
  (init-icat icat-config-path)
  (init-jargon rods-config-path))

(defn init-prod [] (init prod-icat-config prod-jargon-config))
(defn init-qa [] (init qa-icat-config qa-jargon-config))
