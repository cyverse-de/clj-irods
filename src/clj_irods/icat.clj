(ns clj-irods.icat
  (:require [clj-irods.cache-tools :as cache]
            [clojure.tools.logging :as log]
            [clj-icat-direct.icat :as icat]))

;; paged-folder-listing
(defn- paged-folder-listing*
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (let [more-opts {:entity-type entity-type :sort-column sort-column :sort-direction sort-direction :limit limit :offset offset :info-types info-types}]
    (->> [path ::paged-folder-listing zone user entity-type info-types sort-column sort-direction limit offset]
         (cache/cached-or-do (:cache irods) #(apply icat/paged-folder-listing (apply concat (assoc more-opts :user user :zone zone :folder-path path)))))))

(defn paged-folder-listing
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (let [more-opts {:entity-type entity-type :sort-column sort-column :sort-direction sort-direction :limit limit :offset offset :info-types info-types}]
    (->> [path ::paged-folder-listing zone user entity-type info-types sort-column sort-direction limit offset]
         (cache/cached-or-agent (:cache irods) #(apply paged-folder-listing* irods user zone path (apply concat more-opts)) (:icat-pool irods)))))

(defn cached-paged-folder-listing
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types] :or {entity-type :any sort-column :base-name sort-direction :desc limit 1000 offset 0 info-types []}}]
  (let [more-opts {:entity-type entity-type :sort-column sort-column :sort-direction sort-direction :limit limit :offset offset :info-types info-types}]
    (->> [path ::paged-folder-listing zone user entity-type info-types sort-column sort-direction limit offset]
         (cache/cached-or-nil (:cache irods)))))

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
                    (filter (fn [[k v]] (= k this-filter)) nested-map))))))))

(defn filtered-cached-listings
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types]}]
  (let [all (all-cached-listings irods user zone path)]
    (when all
      (let [filters [entity-type info-types sort-column sort-direction limit offset]
            cached  (filter-listings* filters @all)]
        (when (seq cached)
          (delay cached))))))

(defn- keys-in [m]
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

;; NOTE: this merges only directly adjacent ranges -- overlapping ranges will remain separate. This could be less than ideal.
;; e.g. if we have three ranges, (0 30) (10 40) (20 50), none of them will merge even though we have everything in (0 50)
;; I'm hoping this doesn't matter much in practice, since it's usually not all that useful to grab overlapping ranges.
(defn- merge-listing
  [extant to-merge limit offset]
  (let [ranges (into {} (mapcat (fn [[k v]] (mapv #(vector (+ % k) [k %]) (keys v))) extant))]
    (if (contains? ranges offset) ;; is there an existing range that ends at our start point
      (let [[old-limit old-offset] (get ranges offset) ;; there is, so extend it with these new values
            extant-values (get-in extant [old-limit old-offset])
            new-values    (concat extant-values to-merge)]
        (into {} (remove (fn [[k v]] (empty? v))
          (-> extant
              (update-in [old-limit] dissoc old-offset) ;; effectively, dissoc-in [old-limit old-offset]
              (assoc-in [(+ old-limit limit) old-offset] new-values)))))
      (assoc-in extant [limit offset] to-merge)))) ;; there is not, so just stick the merge in

(defn merge-listings
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
  (let [unbounded-limit? (some nil? (keys merged-listings))
        unbounded-offset (and unbounded-limit? (filter #(>= offset %) (keys (get merged-listings nil))))
        bounded-limit (and (not unbounded-limit?) (not (nil? limit)) (filter #(<= limit %) (keys merged-listings)))
        bounded-offset (and (seq bounded-limit) (filter #(>= offset %) (keys (get merged-listings (first bounded-limit)))))
        bounded-limit-offset (and (not (seq bounded-limit)) (filter (fn [[l o]] (and (>= offset o) (> l (count (get-in merged-listings [l o]))))) (map (partial take 2) (keys-in merged-listings))))]
    (log/info "get-range" limit offset unbounded-limit? unbounded-offset bounded-limit bounded-offset bounded-limit-offset)
    (cond
      ;; we have a cached listing with (:limit nil) and an offset <= our requested offset
      (and unbounded-limit? (seq unbounded-offset))
      (let [cached-offset (first unbounded-offset)
            results (get-in merged-listings [nil cached-offset])]
        (if (nil? limit)
          (delay (drop (- offset cached-offset) results))
          (delay (take limit (drop (- offset cached-offset) results)))))

      ;; we have a cached listing where the number of items is less than its limit (i.e. -- it reached the end) and an offset <= our requested offset
      (seq bounded-limit-offset)
      (let [[cached-limit cached-offset] (first bounded-limit-offset)
            results (get-in merged-listings [cached-limit cached-offset])]
        (if (nil? limit)
          (delay (drop (- offset cached-offset) results))
          (delay (take limit (drop (- offset cached-offset) results)))))

      ;; we have a bounded limit and offset in the request, and a cached entry that can fulfill it
      (and (not (nil? limit)) (seq bounded-limit) (seq bounded-offset))
      (let [cached-limit (first bounded-limit)
            cached-offset (first bounded-offset)
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
       (cache/cached-or-nil (:cache irods) #(user-group-ids* irods user zone))))

(defn maybe-user-group-ids
  [irods user zone]
  (delay (deref (user-group-ids irods user zone))))
