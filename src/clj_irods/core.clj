(ns clj-irods.core
  (:require [clj-jargon.init :as init]
            [clj-icat-direct.icat :as icat]
            [clj-irods.cache-tools :as cache]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import [java.util.concurrent Executors ThreadFactory]
           [org.irods.jargon.core.exception FileNotFoundException]))

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
  [use-icat & body]
  `(if ~use-icat
     (icat/with-icat-transaction span-sym#
       (do ~@body))
     (do ~@body)))

(defn make-threadpool
  [prefix thread-count]
  (let [counter (atom 0)]
    (Executors/newFixedThreadPool
      thread-count
      (proxy [ThreadFactory] []
        (newThread [^Runnable runnable]
          (let [t (Thread. runnable)]
            (.setName t (str prefix "-" (swap! counter inc)))
            t))))))

(defmacro with-irods
  "Open connections to iRODS and/or transactions in the ICAT depending on the
  options passed in `cfg` and what's configured.  Bind this to the symbol
  passed in as `sym`. Recommended choice is to call it irods."
  [cfg sym & body]
  `(let [id# (name (gensym "with-irods-"))
         jargon-cfg# (jargon-cfg (:jargon ~cfg))
         use-jargon# (boolean jargon-cfg#)
         use-icat# (have-icat)
         jargon-pool# (or (:combined-pool ~cfg) (:jargon-pool ~cfg) (make-threadpool (str id# "-jargon") (or (:jargon-pool-size ~cfg) 1)))
         icat-pool#   (or (:combined-pool ~cfg) (:icat-pool ~cfg) (make-threadpool (str id# "-icat") (or (:icat-pool-size ~cfg) 5)))]
     (try+
       (maybe-icat-transaction use-icat#
         (maybe-jargon use-jargon# jargon-cfg# jargon#
           (let [~sym {:jargon      jargon#
                       :jargon-pool jargon-pool#
                       :icat-pool   icat-pool#
                       :has-jargon  use-jargon#
                       :has-icat    use-icat#
                       :cache       (atom {})}]
             (do ~@body))))
       (finally
         (when-not (:retain-jargon-pool ~cfg) (.shutdown jargon-pool#))
         (when-not (:retain-icat-pool ~cfg) (.shutdown icat-pool#))))))
