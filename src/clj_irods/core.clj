(ns clj-irods.core
  (:require [clj-jargon.init :as init]
            [clj-icat-direct.icat :as icat-direct]
            [slingshot.slingshot :refer [try+ throw+]]

            [clj-jargon.permissions :as jargon-perms]

            [clj-irods.cache-tools :as cache]
            [clj-irods.jargon :as jargon]
            [clj-irods.icat :as icat]
            [clojure-commons.file-utils :as ft]

            [medley.core :refer [remove-vals]])
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
  [use-jargon jargon-cfg jargon-opts jargon-sym & body]
  `(if ~use-jargon
     (init/with-jargon ~jargon-cfg :lazy true
                                   :client-user (:client-user ~jargon-opts)
                                   :auto-close  (:auto-close ~jargon-opts)
                                   [~jargon-sym]

       (do ~@body))
     (let [~jargon-sym (delay (throw+ {:type :no-irods}))]
      (do ~@body))))

(defmacro maybe-icat-transaction
  [use-icat-transaction & body]
  `(if ~use-icat-transaction
     (icat-direct/with-icat-transaction
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
         jargon-cfg# (or (:jargon-cfg ~cfg) (jargon-cfg (:jargon ~cfg)))
         jargon-opts# (or (:jargon-opts ~cfg) {})
         use-jargon# (boolean jargon-cfg#)
         use-icat# (and (:use-icat ~cfg true) (have-icat))
         use-icat-transaction# (and use-icat# (get ~cfg :use-icat-transaction true))
         jargon-pool# (or (:combined-pool ~cfg) (:jargon-pool ~cfg) (make-threadpool (str id# "-jargon") (or (:jargon-pool-size ~cfg) 1)))
         icat-pool#   (or (:combined-pool ~cfg) (:icat-pool ~cfg) (make-threadpool (str id# "-icat") (or (:icat-pool-size ~cfg) 5)))]
     (try+
      (maybe-icat-transaction use-icat-transaction#
         (maybe-jargon use-jargon# jargon-cfg# jargon-opts# jargon#
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

;; Framework functions to be used to prefer cached values and choose between available data sources.

(defn- from-listing
  [cache? irods extract-fn user zone path]
  (let [get-from-listing (fn [listing] (first (filter #(= (:full_path %) path) (if (delay? listing) @listing listing))))]
    (if cache?
      (let [paged-folder-listing (icat/all-cached-listings irods user zone (ft/dirname path))
            single-item (icat/cached-get-item irods user zone path)
            listing-item (when (delay? paged-folder-listing)
                           (get-from-listing (apply concat (icat/flatten-cached-listings @paged-folder-listing))))] (or
          (when listing-item (delay (extract-fn listing-item)))
          (when single-item  (delay (extract-fn @single-item)))
          nil))
      ;; better if we pass user group IDs in so we can get them from any source, rather than forcing the icat version specifically
      (when (:has-icat irods)
        (let [userids (icat/user-group-ids irods user zone)
              item (future (force userids) @(icat/get-item irods user zone path))]
          (delay (extract-fn @item)))))))

(defn- from-stat
  [cache? irods extract-fn path]
  (if cache?
    (let [jargon-stat (jargon/cached-stat irods path)]
      (when jargon-stat (delay (extract-fn @jargon-stat))))
    (when (:has-jargon irods)
      (let [jargon-stat (jargon/stat irods path)]
        (delay (extract-fn @jargon-stat))))))

(declare object-type)
(defn- from-jargon-metadata
  [cache? irods extract-fn post-fn user zone path]
  (let [get-from-metadata (fn [metadata]
                            (when-let [extracted (extract-fn @metadata)]
                              (delay (post-fn extracted))))]
    (when-let [metadata (if cache?
                          (jargon/cached-get-metadata irods path)
                          (when (:has-jargon irods)
                            (jargon/get-metadata irods path :known-type @(object-type irods user zone path))))]
      (or (get-from-metadata metadata) nil))))

(defn- cached-or-get
  "Takes an irods instance and a set of function-call-like vectors, that is,
  vectors where the first item is a function and the rest are arguments. These
  functions will be called in order, with the arguments `true` and `irods` as
  the first two (before the rest provided), and then with `false` instead.
  Execution halts on the first value that is a `delay`, which is what we get
  back from the cache and from functions that calculate cached values. Values
  not already cached, and unconfigured/disabled data sources should return nil
  instead."
  [irods & get-fns]
  (or
    (first (filter delay?
        (concat
          (map #(apply (first %) true  irods (rest %)) get-fns)
          (map #(apply (first %) false irods (rest %)) get-fns))))
    nil))

(defn- from-stat-or-listing
  "A small nicety function for things that can be simply extracted with small
  functions from either a stat or a listing and don't need special handling."
  [stat-extract-fn listing-extract-fn irods user zone path]
  (cached-or-get irods
    [from-listing listing-extract-fn user zone path]
    [from-stat stat-extract-fn path]))

;; Formatting functions
(defn- object-type-from-listing
  "Determines the object type from an item listing."
  [item]
  (condp = (:type item)
    "dataobject" :file
    "collection" :dir
    :none))

(defn- epoch-millis-from-listing-timestamp
  "Converts a timestamp from a listing, which is a string representation of the number of seconds since the epoch, to the
  number of milliseconds since the epoch."
  [timestamp]
  (* 1000 (Integer/parseInt timestamp)))

(defn- stat-from-listing
  "Formats file stat information from a file listing."
  [listing]
  (when listing
    (let [type (object-type-from-listing listing)]
      (->> {:id            (:full_path listing)
            :path          (:full_path listing)
            :type          type
            :date-created  (epoch-millis-from-listing-timestamp (:create_ts listing))
            :date-modified (epoch-millis-from-listing-timestamp (:modify_ts listing))
            :md5           (when (= type :file) (:data_checksum listing))
            :file-size     (when (= type :file) (:data_size listing))}
           (remove-vals nil?)))))

;; Actual API functions
(defn invalidate
  "Removes a provided path, uuid, user, etc. from the cache. Can be either a single key or a vector path into the cache.

  Generally, pass a path, uuid, or username."
  [irods to-invalidate]
  (cache/clear-cache-prefix (:cache irods) (if (vector? to-invalidate) to-invalidate [to-invalidate])))

(defn object-type
  "The type of the object. Returns a keyword, :file :dir or :none"
  [irods user zone path]
  (let [listing-extract-fn (fn [item] (condp = (:type item) "dataobject" :file "collection" :dir :none))]
    (from-stat-or-listing :type listing-extract-fn irods user zone path)))

(defn date-modified
  "The modified date of the path, as milliseconds since the epoch"
  [irods user zone path]
  (from-stat-or-listing :date-modified (comp epoch-millis-from-listing-timestamp :modify_ts) irods user zone path))

(defn date-created
  "The created date of the path, as milliseconds since the epoch"
  [irods user zone path]
  (from-stat-or-listing :date-created (comp epoch-millis-from-listing-timestamp :create_ts) irods user zone path))

(defn file-size
  "The number of bytes of a path. 0 for folders."
  [irods user zone path]
  (from-stat-or-listing :file-size :data_size irods user zone path))

(defn stat
  "Item stat information for a path."
  [irods user zone path]
  (from-stat-or-listing identity stat-from-listing irods user zone path))

(defn object-avu
  "Get a specific AVU or set of AVUs for a path, filtering on attribute, value, and/or unit.

  Returns in [{:attr x :value y :unit z} ...] format (like clj-jargon.metadata/avu2map)"
  [irods user zone path avu]
  (cached-or-get irods
    [(fn [cache? irods path user zone avu]
       (let [match-avu (fn [to-test]
                         (cond
                           (and (:attr avu) (not (= (:attr to-test) (:attr avu))))    false
                           (and (:value avu) (not (= (:value to-test) (:value avu)))) false
                           (and (:unit avu) (not (= (:unit to-test) (:unit avu))))    false
                           :else                                                      true))
             get-avu (fn [metadata] (let [found (filter match-avu @metadata)]
                                      (when (seq found)
                                        (delay found))))]
         (when-let [metadata (if cache?
                               (jargon/cached-get-metadata irods path)
                               (when (:has-jargon irods)
                                 (jargon/get-metadata irods path :known-type @(object-type irods user zone path))))]
           (get-avu metadata)))) path user zone avu]))

(def uuid-attr "ipc_UUID")
(def info-type-attr "ipc-filetype")

(defn uuid
  "Get the UUID (via the ipc_UUID AVU) for a path."
  [irods user zone path]
  (cached-or-get irods
    [from-listing :uuid user zone path]
    [from-jargon-metadata (fn [metadata] (first (filter #(= (:attr %) uuid-attr) metadata)))
     #(get % :value) user zone path]))

(defn info-type
  [irods user zone path]
  (cached-or-get irods
    [from-listing :info_type user zone path]
    [from-jargon-metadata (fn [metadata] (first (filter #(= (:attr %) info-type-attr) metadata)))
     #(get % :value) user zone path]))

(defn permission
  "Permission for a user on a path. Returned as a keyword like :read :write :own, or nil"
  [irods user zone path]
  (cached-or-get irods
    [from-listing (fn [p] (if-let [p (:access_type_id p)] (jargon-perms/fmt-perm p) p)) user zone path]
    [(fn [cache? irods user path]
       (if cache?
         (jargon/cached-permission-for irods user path)
         (when (:has-jargon irods) (delay
                                     @(jargon/permission-for irods user path :known-type @(object-type irods user zone path))))))
     user path]))

(defn folder-listing
  "Get a listing. Because listings can be paged as well as filtered, this
  function merges abutting pages for compatible sets of filters among cached
  listings, then determines if the merged values can satisfy the request,
  selecting the specific section requested out of the merged values. When this
  is not possible, makes a new request instead."
  [irods user zone path &
   {:keys [entity-type sort-column sort-direction limit offset info-types]
    :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
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
      (when (:has-icat irods)
        (let [userids (icat/user-group-ids irods user zone)]
          (delay
            (force userids)
            @(icat/paged-folder-listing irods user zone path
                                        :entity-type entity-type
                                        :sort-column sort-column
                                        :sort-direction sort-direction
                                        :limit limit
                                        :offset offset
                                        :info-types info-types)))))))

(defn items-in-folder
  "Count of items in folder. This uses a listing under the hood, and considers
  all cached listings with compatible filters, since they should have the same
  number of items with equivalent filters, no matter what page is requested."
  [irods user zone path &
   {:keys [entity-type info-types]
    :or {entity-type :any info-types []}}] ;; only support the actual filters, we don't care about sorting and limit/offset for this
  (let [cached (icat/filtered-cached-listings irods user zone path :entity-type entity-type :info-types info-types)]
    (delay (get-in (first
                    (if cached
                      (first (icat/flatten-cached-listings @cached))
                      (when (:has-icat irods) (deref (icat/paged-folder-listing irods user zone path :entity-type entity-type :info-types info-types)))))
                   [:total_count]))))

(defn list-folders-in-folder
  "Get a listing of folders within a specified folder. This is done for a very
  specific use case, so the results cannot be paged or filtered. This function
  is not supported in Jargon yet."
  [irods user zone path]
  (cached-or-get irods
    [(fn [cache? irods user zone path]
       (when-let [folders (if cache?
                            (icat/cached-folders-in-folder irods user zone path)
                            (when (:has-icat irods)
                              (icat/folders-in-folder irods user zone path)))]
         (delay @folders))) user zone path]))

(defn user-type
  "The user type associated with a username. :none if the user does not exist, theoretically"
  [irods username zone]
  (cached-or-get irods
    [(fn [cache? irods username zone]
       (when-let [user (if cache?
                         (icat/cached-user irods username zone)
                         (when (:has-icat irods)
                           (icat/user irods username zone)))]
         (delay (if (:user_type_name @user) :user :none)))) username zone] ; for now mimicking jargon version that doesn't distingush
    [(fn [cache? irods username zone]
       (when-let [user (if cache?
                         (jargon/cached-get-user irods username zone)
                         (when (:has-jargon irods)
                           (jargon/get-user irods username zone)))]
         (delay (:type @user)))) username zone]))

(defn uuid->path
  "The path associated with a uuid, or nil."
  [irods uuid]
  (cached-or-get irods
    [(fn [cache? irods uuid]
       (if cache?
         (icat/cached-path-for-uuid irods uuid)
         (when (:has-icat irods)
           (icat/path-for-uuid irods uuid)))) uuid]
    [(fn [cache? irods uuid]
       (if cache?
         (jargon/cached-get-path irods uuid)
         (when (:has-jargon irods)
           (jargon/get-path irods uuid)))) uuid]))

;; TODO: this isn't quite as optimized as it could be. For example, if the paths for all of the UUIDs
;; have already been cached by calls to Jargon then the ICAT query will still occur even though it's
;; unnecessary. This should be fixed eventually.
(defn uuids->paths
  "The paths associated with multiple uuids, returned as a map from uuid to
  the corresponding path or nil"
  [irods uuids]
  (cached-or-get irods
    [(fn [cache? irods uuids]
       (when (:has-icat irods)
         (icat/paths-for-uuids irods uuids))) uuids]
    [(fn [cache? irods uuids]
       (when (:has-jargon irods)
         (jargon/get-paths irods uuids))) uuids]))

(defn list-user-permissions
  "Lists permissions for all users on a path."
  [irods path]
  (cached-or-get irods
    [(fn [cache? irods path]
       (let [format-perm (fn [{user :user access-type-id :access_type_id}]
                           {:user user :permissions (jargon-perms/perm-map-for (str access-type-id))})]
         (if cache?
           (when-let [perms (icat/cached-list-perms-for-item irods path)]
             (delay (mapv format-perm @perms)))
           (when (:has-icat irods)
             (delay (mapv format-perm @(icat/list-perms-for-item irods path))))))) path]
    [(fn [cache? irods path]
       (if cache?
         (jargon/cached-list-user-perms irods path)
         (when (:has-jargon irods)
           (jargon/list-user-perms irods path)))) path]))

(defn number-of-files-in-folder
  "Counts the number of files within a folder, excluding subfolders."
  [irods user zone path]
  (cached-or-get irods
    [(fn [cache? irods user zone path]
       (if cache?
         (icat/cached-number-of-files-in-folder irods user zone path)
         (when (:has-icat irods)
           (icat/number-of-files-in-folder irods user zone path)))) user zone path]
    [(fn [cache? irods user zone path]
       (if cache?
         (jargon/cached-num-dataobjects-under-path irods user zone path)
         (when (:has-jargon irods)
           (jargon/num-dataobjects-under-path irods user zone path)))) user zone path]))

(defn number-of-folders-in-folder
  "Counts the number of folders within a folder, excluding subfolders."
  [irods user zone path]
  (cached-or-get irods
    [(fn [cache? irods user zone path]
       (if cache?
         (icat/cached-number-of-folders-in-folder irods user zone path)
         (when (:has-icat irods)
           (icat/number-of-folders-in-folder irods user zone path)))) user zone path]
    [(fn [cache? irods user zone path]
       (if cache?
         (jargon/cached-num-collections-under-path irods user zone path)
         (when (:has-jargon irods)
           (jargon/num-collections-under-path irods user zone path)))) user zone path]))
