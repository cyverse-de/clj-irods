(ns clj-irods.core
  (:require [clj-jargon.init :as init]
            [clj-icat-direct.icat :as icat-direct]
            [slingshot.slingshot :refer [try+ throw+]]

            [clojure.tools.logging :as log]

            [clj-irods.jargon :as jargon]
            [clj-irods.icat :as icat]
            [clojure-commons.file-utils :as ft]
            )
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
               (icat-direct/icat-db-spec
                 (:host c)
                 (:user c)
                 (:password c)
                 :port (:port c)
                 :db (:db c))))))

(defn have-icat
  []
  (map? icat-direct/icat))

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
     (icat-direct/with-icat-transaction span-sym#
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

(defn field-from-stat-or-listing
  [stat-field get-from-listing-item irods user zone path]
  (let [get-from-listing (fn [listing] (first (filter #(= (:full_path %) path) @listing)))
        paged-folder-listing (icat/cached-paged-folder-listing irods user zone (ft/dirname path))
        listing-item (when (delay? paged-folder-listing) (get-from-listing paged-folder-listing))
        jargon-stat (jargon/cached-stat irods path)]
    (or
      (when listing-item (delay (get-from-listing-item listing-item)))
      (when jargon-stat (delay (get-in @jargon-stat [stat-field])))
      (when (:has-jargon irods) (delay (get-in @(jargon/stat irods path) [stat-field])))
      ;; listing last because the default limit could exclude the object we care about
      (when (:has-icat irods) (delay (get-from-listing-item (get-from-listing (icat/paged-folder-listing irods user zone (ft/dirname path)))))))))

(defn object-type
  [irods user zone path]
  (let [stat-field :type
        get-from-listing-item (fn [item] (condp = (:type item) "dataobject" :file "collection" :dir nil))]
    (field-from-stat-or-listing stat-field get-from-listing-item irods user zone path)))

(defn date-modified
  [irods user zone path]
  (field-from-stat-or-listing :date-modified :modify_ts irods user zone path))

(defn date-created
  [irods user zone path]
  (field-from-stat-or-listing :date-created :create_ts irods user zone path))

(defn file-size
  [irods user zone path]
  (field-from-stat-or-listing :file-size :data_size irods user zone path))

(defn items-in-folder
  [irods user zone path & {:keys [entity-type info-types] :or {entity-type :any info-types []}}] ;; only support the actual filters, we don't care about sorting and limit/offset for this
  (let [cached (icat/filtered-cached-listings irods user zone path :entity-type entity-type :info-types info-types)]
    (delay (get-in (first
                     (if cached
                       (first (icat/flatten-cached-listings @cached))
                       (deref (icat/paged-folder-listing irods user zone path :entity-type entity-type :info-types info-types))))
                   [:total_count]))))
