(ns clj-irods.core
  (:require [clj-jargon.init :as init]
            [clj-icat-direct.icat :as icat-direct]
            [slingshot.slingshot :refer [try+ throw+]]

            [clojure.tools.logging :as log]

            [clj-jargon.permissions :as jargon-perms]

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

(defn- cached-or-get
  [irods & get-fns]
  (or
    (first (filter delay?
        (concat
          (map #(apply (first %) true  irods (rest %)) get-fns)
          (map #(apply (first %) false irods (rest %)) get-fns))))
    nil))

(defn- from-listing
  [cache? irods extract-fn user zone path]
  (let [get-from-listing (fn [listing] (first (filter #(= (:full_path %) path) (if (delay? listing) @listing listing))))]
    (if cache?
      (let [paged-folder-listing (icat/all-cached-listings irods user zone (ft/dirname path))
            single-item (icat/cached-get-item irods user zone path)
            listing-item (when (delay? paged-folder-listing) (get-from-listing (apply concat (icat/flatten-cached-listings @paged-folder-listing))))]
        (or
          (when listing-item (delay (extract-fn listing-item)))
          (when single-item  (delay (extract-fn @single-item)))
          nil))
      (or
        ;; better if we pass user group IDs in so we can get them from any source, rather than forcing the icat version specifically
        (when (:has-icat irods) (delay (force (icat/user-group-ids irods user zone)) (extract-fn @(icat/get-item irods user zone path))))
        nil))))

(defn- from-stat
  [cache? irods extract-fn path]
  (if cache?
    (let [jargon-stat (jargon/cached-stat irods path)]
      (or
        (when jargon-stat (delay (extract-fn @jargon-stat)))
        nil))
    (or
      (when (:has-jargon irods) (delay (extract-fn @(jargon/stat irods path))))
      nil)))

(defn- from-stat-or-listing
  [stat-extract-fn listing-extract-fn irods user zone path]
  (cached-or-get irods
    [from-listing listing-extract-fn user zone path]
    [from-stat stat-extract-fn path]))

(defn object-type
  [irods user zone path]
  (let [listing-extract-fn (fn [item] (condp = (:type item) "dataobject" :file "collection" :dir nil))]
    (from-stat-or-listing :type listing-extract-fn irods user zone path)))

(defn date-modified
  [irods user zone path]
  (from-stat-or-listing :date-modified (comp (partial * 1000) #(Integer/parseInt %) :modify_ts) irods user zone path))

(defn date-created
  [irods user zone path]
  (from-stat-or-listing :date-created (comp (partial * 1000) #(Integer/parseInt %) :create_ts) irods user zone path))

(defn file-size
  [irods user zone path]
  (from-stat-or-listing :file-size :data_size irods user zone path))

(defn uuid
  [irods user zone path]
  (cached-or-get irods
    [from-listing :uuid user zone path]))

(defn permission
  [irods user zone path]
  (cached-or-get irods
    [from-listing (comp jargon-perms/fmt-perm :access_type_id) user zone path]
    [(fn [cache? irods user path]
       (if cache?
         (jargon/cached-permission-for irods user path)
         (delay
           @(jargon/permission-for irods user path :known-type @(object-type irods user zone path)))))
     user path]))

(defn folder-listing
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (let [cached (get-in
                 (icat/merge-listings
                   (icat/filtered-cached-listings irods user zone path
                                                  :entity-type entity-type
                                                  :sort-column sort-column
                                                  :sort-direction sort-direction
                                                  :info-types info-types))
                 [entity-type info-types sort-column sort-direction]) ;; don't include limit/offset here
        cached-range (and cached (icat/get-range cached limit offset))]
    (if cached-range
      cached-range
      (icat/paged-folder-listing irods user zone path :entity-type entity-type :sort-column sort-column :sort-direction sort-direction :limit limit :offset offset :info-types info-types))))

(defn items-in-folder
  [irods user zone path & {:keys [entity-type info-types] :or {entity-type :any info-types []}}] ;; only support the actual filters, we don't care about sorting and limit/offset for this
  (let [cached (icat/filtered-cached-listings irods user zone path :entity-type entity-type :info-types info-types)]
    (delay (get-in (first
                     (if cached
                       (first (icat/flatten-cached-listings @cached))
                       (deref (icat/paged-folder-listing irods user zone path :entity-type entity-type :info-types info-types))))
                   [:total_count]))))
