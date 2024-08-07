(ns clj-irods.icat
  "This namespace contains the assorted caching and fetching functions wrapping
  clj-icat-direct. In general, it should be used mostly by clj-irods.core, and
  not by users directly."
  (:require [medley.core :refer [dissoc-in]]
            [clj-irods.cache-tools :as cache]
            [clojure-commons.file-utils :as ft]
            [clj-icat-direct.icat :as icat]))

;; user
(defn- user*
  [irods username zone]
  (->> [username zone ::user]
       (cache/cached-or-do (:cache irods) #(icat/user username zone))))

(defn user
  [irods username zone]
  (->> [username zone ::user]
       (cache/cached-or-agent (:cache irods) #(user* irods username zone) (:icat-pool irods))))

(defn cached-user
  [irods username zone]
  (->> [username zone ::user]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-user
  [irods username zone]
  (delay (deref (user irods username zone))))

;; user-group-ids
(defn- user-group-ids*
  [irods user zone]
  (->> [user zone ::user-group-ids]
       (cache/cached-or-do (:cache irods) #(icat/user-group-ids user zone))))

(defn user-group-ids
  [irods user zone]
  (->> [user zone ::user-group-ids]
       (cache/cached-or-agent (:cache irods) #(user-group-ids* irods user zone) (:icat-pool irods))))

(defn cached-user-group-ids
  [irods user zone]
  (->> [user zone ::user-group-ids]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-user-group-ids
  [irods user zone]
  (delay (deref (user-group-ids irods user zone))))

;; paged-folder-listing
(defn- paged-folder-listing*
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types user-group-ids] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (let [cached-ids (cached-user-group-ids irods user zone)
        user-group-ids (if cached-ids @cached-ids user-group-ids)
        more-opts {:entity-type entity-type :sort-column sort-column :sort-direction sort-direction :limit limit :offset offset :info-types info-types :user-group-ids user-group-ids}]
    (->> [path ::paged-folder-listing zone user entity-type info-types sort-column sort-direction limit offset]
         (cache/cached-or-do (:cache irods) #(apply icat/paged-folder-listing (apply concat (assoc more-opts :user user :zone zone :folder-path path)))))))

(defn paged-folder-listing
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types user-group-ids] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (let [more-opts {:entity-type entity-type :sort-column sort-column :sort-direction sort-direction :limit limit :offset offset :info-types info-types :user-group-ids user-group-ids}]
    (->> [path ::paged-folder-listing zone user entity-type info-types sort-column sort-direction limit offset]
         (cache/cached-or-agent (:cache irods) #(apply paged-folder-listing* irods user zone path (apply concat more-opts)) (:icat-pool irods)))))

(defn cached-paged-folder-listing
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (->> [path ::paged-folder-listing zone user entity-type info-types sort-column sort-direction limit offset]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-paged-folder-listing
  [& args]
  (delay (deref (apply paged-folder-listing args))))

(defn- resolve*
  [depth nested-map]
  (into {} (map
             (fn [[k v]] [k (if (= depth 0) (if (delay? v) (cache/rethrow-if-error @v) v) (resolve* (dec depth) v))])
             nested-map)))

(defn all-cached-listings
  [irods user zone path]
  (let [all (get-in @(:cache irods) [path ::paged-folder-listing zone user])]
    (when (seq all)
      (delay
        (resolve* 5 all)))))

(defn- filter-listings*
  ([filters nested-map]
   (filter-listings* (count filters) filters nested-map))
  ([depth filters nested-map]
   (if (zero? depth)
     nested-map
     (let [this-filter (nth filters (- (count filters) depth))]
       (into {}
             (map (fn [[k v]] [k (filter-listings* (dec depth) filters v)])
                  (if (nil? this-filter)
                    nested-map
                    (filter (fn [[k _v]] (= k this-filter)) nested-map))))))))

(defn filtered-cached-listings
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types]}]
  (let [all (all-cached-listings irods user zone path)]
    (when all
      (let [filters [entity-type info-types sort-column sort-direction limit offset]
            cached  (filter-listings* filters @all)]
        (when (seq cached)
          (delay cached))))))

(defn- keys-in
  "Returns a vector of vectors that are keys into a nested map, compatible with
  get-in, assoc-in, and our cache system. Along with `take` and `drop`, useful
  for merging and splitting cached data."
  [m]
  (if (map? m)
    (vec
      (mapcat (fn [[k v]]
                (let [sub (keys-in v)
                      nested (map #(into [k] %) (filter (comp not empty?) sub))]
                  (if (seq nested)
                    nested
                    [[k]])))
              m))
    []))

(defn- merge-listing
  "Take a partially-merged output, a listing, and that listing's limit and
  offset. Return a new output, where if the new listing is directly adjacent to
  an existing thing in the cache, it is merged with it.

  Note that this merges only directly adjacent ranges -- overlapping ranges
  will remain separate. This could be less than ideal.  e.g. if we have three
  ranges, (0 30) (10 40) (20 50), none of them will merge even though we have
  everything in (0 50) This will hopefully not matter much in practice, since
  it's usually not all that useful to grab overlapping ranges."
  [extant to-merge limit offset]
  (let [ranges (into {} (mapcat (fn [[k v]] (mapv #(vector (+ % k) [k %]) (keys v))) extant))]
    (if (contains? ranges offset) ;; is there an existing range that ends at our start point
      (let [[old-limit old-offset] (get ranges offset) ;; there is, so extend it with these new values
            extant-values (get-in extant [old-limit old-offset])
            new-values    (concat extant-values to-merge)]
          (-> extant
              (dissoc-in [old-limit old-offset])
              (assoc-in [(if (nil? limit) nil (+ old-limit limit)) old-offset] new-values)))
      (assoc-in extant [limit offset] to-merge)))) ;; there is not, so just stick the merge in

(defn merge-listings
  "Given a set of listings, merge them together. Get the set of paths into the
  object with `keys-in`, trim to 6 (the four filters plus limit and offset),
  then reduce over them. If the output already has information for the first 4
  components of the key (the filters, but not limit or offset), then merge with
  the current value using `merge-listing`, otherwise copy the value directly.

  This probably doesn't work entirely if things come in weirder orders, but the
  worst case is an extra database call, not outright brokenness."
  [listings]
  (let [listings (if (delay? listings) @listings listings)
        key-paths (map (partial take 6) (keys-in listings))]
    (reduce
      (fn [m ks]
        (let [ks4            (take 4 ks)
              [limit offset] (drop 4 ks)]
          (if-let [extant (get-in m ks4)]
            (assoc-in m ks4 (merge-listing extant (get-in listings ks) limit offset))
            (assoc-in m ks (get-in listings ks)))))
      {}
      key-paths)))

(defn get-range
  "Given merged listings selected down to limit-offset section, get a section of results for a limit & offset"
  [merged-listings limit offset]
  (let [all-ranges (mapv (partial take 2) (keys-in merged-listings))
        ;; which routes into merged-listings have a limit of `nil` and an offset less than or equal to our request?
        unbounded-limit-offset (filter (fn [[l o]] (and (nil? l) (>= offset o))) all-ranges)
        ;; which routes into merged-listings have a limit high enough to fulfill our (bounded) request and an acceptable offset?
        bounded-limit-offset (and
                               (not (nil? limit))
                               (filter
                                 (fn [[l o]] (and
                                               (not (nil? l)) ;; bounded limit
                                               (>= offset o)  ;; acceptable offset
                                               (<= (+ limit (- offset o)) l))) ;; cached limit must cover both the request limit and the difference in offsets
                                 all-ranges))
        ;; which routes into merged-listings have reached the end of the results and have an acceptable offset?
        finished-ranges (filter (fn [[l o]] (and (not (nil? l)) (>= offset o) (> l (count (get-in merged-listings [l o]))))) all-ranges)]
    (cond
      ;; we have a cached listing with (:limit nil) and an offset <= our requested offset
      (seq unbounded-limit-offset)
      (let [[cached-limit cached-offset] (first unbounded-limit-offset)
            results (get-in merged-listings [cached-limit cached-offset])]
        (if (nil? limit)
          (delay (drop (- offset cached-offset) results))
          (delay (take limit (drop (- offset cached-offset) results)))))

      ;; we have a cached listing where the number of items is less than its limit (i.e. -- it reached the end) and an offset <= our requested offset
      (seq finished-ranges)
      (let [[cached-limit cached-offset] (first finished-ranges)
            results (get-in merged-listings [cached-limit cached-offset])]
        (if (nil? limit)
          (delay (drop (- offset cached-offset) results))
          (delay (take limit (drop (- offset cached-offset) results)))))

      ;; we have a bounded limit and offset in the request, and a cached entry that can fulfill it
      (and (not (nil? limit)) (seq bounded-limit-offset))
      (let [[cached-limit cached-offset] (first bounded-limit-offset)
            results (get-in merged-listings [cached-limit cached-offset])]
        (delay (take limit (drop (- offset cached-offset) results))))

      :else
      nil)))

;; cached offset 10 limit 10, request offset 15 limit 5 vs limit 10
;; 10 - (15 - 10) = 5 = maximum limit we can do, i.e. must be <= request limit
;; cached offset 0 limit 1000; request offset 100 limit 100
;; (1000 - (100 - 0) = 900)

;; listings are {limit {offset [seq]}}
;; our offset must be <= the offset of the merged+cached listing, regardless
;; if our limit is nil, then either the cached limit must be nil, or (cached-limit + cached-offset > actual # of items in cached listing)
;; if our limit is specified, then (cached-limit - (our-offset - cached-offset)) >= our-limit, or cached-limit must be nil

(defn flatten-cached-listings
  "Take a set of listings as they're cached (from all-cached-listings or filtered-cached-listings) and return a simple sequence"
  [listings]
  (->> listings
       vals            ; entity-type
       (mapcat vals)   ; info-types
       (mapcat vals)   ; sort-column
       (mapcat vals)   ; sort-direction
       (mapcat vals)   ; limit
       (mapcat vals))) ; offset

;; get-item
(defn- get-item*
  [irods user zone path & {:keys [user-group-ids]}]
  (let [cached-ids (cached-user-group-ids irods user zone)
        args (cond
               (seq user-group-ids) [user-group-ids]
               cached-ids           [@cached-ids]
               :else                [user zone])]
    (->> [path ::get-item user zone]
         (cache/cached-or-do (:cache irods)
                             #(apply icat/get-item (ft/dirname path) (ft/basename path) args)))))

(defn get-item
  [irods user zone path & {:keys [user-group-ids]}]
  (->> [path ::get-item user zone]
       (cache/cached-or-agent (:cache irods) #(get-item* irods user zone path :user-group-ids user-group-ids) (:icat-pool irods))))

(defn cached-get-item
  [irods user zone path & _ignored]
  (->> [path ::get-item user zone]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-get-item
  [irods user zone path & {:keys [user-group-ids]}]
  (delay (deref (get-item irods user zone path :user-group-ids user-group-ids))))

;; path-for-uuid
(defn- path-for-uuid*
  [irods uuid]
    (->> [uuid ::path-for-uuid]
         (cache/cached-or-do (:cache irods) #(icat/path-for-uuid uuid))))

(defn path-for-uuid
  [irods uuid]
  (->> [uuid ::path-for-uuid]
       (cache/cached-or-agent (:cache irods) #(path-for-uuid* irods uuid) (:icat-pool irods))))

(defn cached-path-for-uuid
  [irods uuid]
  (->> [uuid ::path-for-uuid]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-path-for-uuid
  [irods uuid]
  (delay (deref (path-for-uuid irods uuid))))

;; paths-for-uuids
(defn paths-for-uuids
  [irods uuids]
  (cache/cached-or-retrieved-values
   (:cache irods)
   icat/paths-for-uuids
   (:icat-pool irods)
   #(vector (str %) ::path-for-uuid)
   str
   uuids))

;; list-perms-for-item
(defn- list-perms-for-item*
  [irods path]
  (->> [path ::list-perms-for-item]
       (cache/cached-or-do (:cache irods) #(icat/list-perms-for-item path))))

(defn list-perms-for-item
  [irods path]
  (->> [path ::list-perms-for-item]
       (cache/cached-or-agent (:cache irods) #(list-perms-for-item* irods path) (:icat-pool irods))))

(defn cached-list-perms-for-item
  [irods path]
  (->> [path ::list-perms-for-item]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-list-perms-for-item
  [irods path]
  (delay (deref (list-perms-for-item irods path))))

;; number-of-files-in-folder
(defn- number-of-files-in-folder*
  [irods user zone path]
  (->> [user zone path ::number-of-files-in-folder]
       (cache/cached-or-do (:cache irods) #(icat/number-of-files-in-folder user zone path))))

(defn number-of-files-in-folder
  [irods user zone path]
  (->> [user zone path ::number-of-files-in-folder]
       (cache/cached-or-agent (:cache irods) #(number-of-files-in-folder* irods user zone path) (:icat-pool irods))))

(defn cached-number-of-files-in-folder
  [irods user zone path]
  (->> [user zone path ::number-of-files-in-folder]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-number-of-files-in-folder
  [irods user zone path]
  (delay (deref (number-of-files-in-folder irods user zone path))))

;; number-of-folders-in-folder
(defn- number-of-folders-in-folder*
  [irods user zone path]
  (->> [user zone path ::number-of-folders-in-folder]
       (cache/cached-or-do (:cache irods) #(icat/number-of-folders-in-folder user zone path))))

(defn number-of-folders-in-folder
  [irods user zone path]
  (->> [user zone path ::number-of-folders-in-folder]
       (cache/cached-or-agent (:cache irods) #(number-of-folders-in-folder* irods user zone path) (:icat-pool irods))))

(defn cached-number-of-folders-in-folder
  [irods user zone path]
  (->> [user zone path ::number-of-folders-in-folder]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-number-of-folders-in-folder
  [irods user zone path]
  (delay (deref (number-of-folders-in-folder irods user zone path))))

;; folders-in-folder
(defn- folders-in-folder*
  [irods user zone path]
  (->> [user zone path ::folders-in-folder]
       (cache/cached-or-do (:cache irods) #(icat/list-folders-in-folder user zone path))))

(defn folders-in-folder
  [irods user zone path]
  (->> [user zone path ::folders-in-folder]
       (cache/cached-or-agent (:cache irods) #(folders-in-folder* irods user zone path) (:icat-pool irods))))

(defn cached-folders-in-folder
  [irods user zone path]
  (->> [user zone path ::folders-in-folder]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-folders-in-folder
  [irods user zone path]
  (delay (deref (folders-in-folder irods user zone path))))
