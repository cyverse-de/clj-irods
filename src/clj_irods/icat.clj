(ns clj-irods.icat
  (:require [clj-irods.cache-tools :as cache]
            [clojure.tools.logging :as log]
            [clj-icat-direct.icat :as icat]))

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
  [irods user zone path & {:keys [entity-type sort-column sort-direction limit offset info-types] :as more-opts}]
  (let [all (get-in @(:cache irods) [path ::paged-folder-listing zone user])]
    (when (seq all)
    (delay
      (resolve* 5 all)))))
