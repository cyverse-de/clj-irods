(ns clj-irods.jargon
  "This namespace contains the assorted caching and fetching functions wrapping
  clj-jargon. In general, it should be used mostly by clj-irods.core, and not
  by users directly."
  (:require [slingshot.slingshot :refer [try+]]
            [clj-jargon.item-info :as info]
            [clj-jargon.permissions :as perms]
            [clj-jargon.metadata :as metadata]
            [clj-irods.cache-tools :as cache])
  (:import [org.irods.jargon.core.exception FileNotFoundException]))

(defn- stat*
  "Creates and caches a delay for a stat of the path. Run and deref in an
  appropriate thread."
  [irods path]
  (->> [path ::stat]
       (cache/cached-or-do (:cache irods) #(info/stat @(:jargon irods) path))))

(defn stat
  "Creates and caches a delay for a stat of the path with `stat*` and tells it
  to start running in the jargon thread pool, returning a `delay` that will
  wait for and then return the stat or rethrow an error."
  [irods path]
  (->> [path ::stat]
       (cache/cached-or-agent (:cache irods) #(stat* irods path) (:jargon-pool irods))))

(defn cached-stat
  "Returns an already-cached `stat` in a delay, or nil."
  [irods path]
  (cache/cached-or-nil (:cache irods) [path ::stat]))

(defn maybe-stat
  "Like `stat`, but an extra wrapper to delay agent computation until deref.
  Useful for establishing a variable that may or may not be used later in a
  with-irods block."
  [irods path]
  (delay (deref (stat irods path))))

(defn- permission-for*
  [irods user path & {:keys [known-type]}]
  (->> [path ::permission-for user]
       (cache/cached-or-do (:cache irods) #(perms/permission-for @(:jargon irods) user path :known-type known-type))))

(defn permission-for
  [irods user path & {:keys [known-type]}]
  (->> [path ::permission-for user]
       (cache/cached-or-agent (:cache irods) #(permission-for* irods user path :known-type known-type) (:jargon-pool irods))))

(defn cached-permission-for
  [irods user path & _ignored_info]
  (->> [path ::permission-for user]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-permission-for
  [irods user path & {:keys [known-type]}]
  (delay (deref (permission-for irods user path :known-type known-type))))

(defn- get-metadata*
  [irods path & {:keys [known-type]}]
  (->> [path ::get-metadata]
       (cache/cached-or-do (:cache irods) #(metadata/get-metadata @(:jargon irods) path))))

(defn get-metadata
  [irods path & {:keys [known-type]}]
  (->> [path ::get-metadata]
       (cache/cached-or-agent (:cache irods) #(get-metadata* irods path :known-type known-type) (:jargon-pool irods))))

(defn cached-get-metadata
  [irods path & _ignored_info]
  (->> [path ::get-metadata]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-get-metadata
  [irods user path & {:keys [known-type]}]
  (delay (deref (get-metadata irods user path :known-type known-type))))
