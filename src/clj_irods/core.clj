(ns clj-irods.core
  (:require [clj-jargon.init :as init]
            [clj-icat-direct.icat :as icat]
            [slingshot.slingshot :refer [throw+]]))

(def jargon-cfg
  (memoize (fn [c]
             (when c
               (init/init
                 (:host c)
                 (:port c)
                 (:username c)
                 (:password c)
                 (:home c)
                 (:zone c)
                 (:resource c)
                 :max-retries (:max-retries c)
                 :retry-sleep (:retry-sleep c)
                 :use-trash (:use-trash c))))))

(def icat-spec
  (memoize (fn [c]
             (when c
               (icat/icat-db-spec
                 (:host c)
                 (:user c)
                 (:password c)
                 :port (:port c)
                 :db (:db c))))))

(defn have-icat
  []
  (map? icat/icat))

(defmacro maybe-jargon
  [use-jargon jargon-cfg jargon-sym & body]
  `(if ~use-jargon
     (init/with-jargon ~jargon-cfg :lazy true [~jargon-sym]
       (do ~@body))
     (let [~jargon-sym (delay (throw+ {:type :no-irods}))]
      (do ~@body))))

(defmacro maybe-icat-transaction
  [use-icat span-sym & body]
  `(if ~use-icat
     (icat/with-icat-transaction ~span-sym
       (do ~@body))
     (let [~span-sym nil]
       (do ~@body))))

(defmacro with-irods
  "Open connections to iRODS and/or transactions in the ICAT depending on the
  options passed in `cfg` and what's configured.  Bind this to the symbol
  passed in as `sym`. Recommended choice is to call it irods."
  [cfg sym & body]
  `(let [jargon-cfg# (jargon-cfg (:jargon ~cfg))
         use-jargon# (boolean jargon-cfg#)
         use-icat# (have-icat)]
     (maybe-icat-transaction use-icat# span#
       (maybe-jargon use-jargon# jargon-cfg# jargon#
         (let [~sym {:jargon     jargon#
                     :has-jargon use-jargon#
                     :has-icat   use-icat#
                     :db-span    span#}]
           (do ~@body))))))
