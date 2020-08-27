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
  (let [key-paths (map (partial take 6) (keys-in listings))]
    (reduce
      (fn [m ks]
        (let [ks4            (take 4 ks)
              [limit offset] (drop 4 ks)]
          (if-let [extant (get-in m ks4)]
            (assoc-in m ks4 (merge-listing extant (get-in listings ks) limit offset))
            (assoc-in m ks (get-in listings ks)))))
      {}
      key-paths)))

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
