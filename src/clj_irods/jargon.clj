(ns clj-irods.jargon
  (:require [slingshot.slingshot :refer [try+]]
            [clj-jargon.item-info :as info]
            [clj-jargon.permissions :as perms]
            [clj-irods.cache-tools :as cache])
  (:import [org.irods.jargon.core.exception FileNotFoundException]))

;; This namespace is probably mostly going to be structured as sets of
;; functions, one (private) using cached-or-do and calling to jargon (creating
;; a delay that does the actual work), the second using cached-or-agent and
;; calling the first function, that handles running things in appropriate
;; thread pools and initiates actual realization of values, the third returning
;; only if the value is already cached, and the last wrapping the second in
;; another delay-deref to delay computation.

;; Cache keys should be by path or ID followed by namespaced keywords
;; (::whatever) so it's clear it corresponds to this namespace.

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
  [irods user path]
  (->> [path ::permission-for user]
       (cache/cached-or-do (:cache irods) #(perms/permission-for @(:jargon irods) user path))))

(defn permission-for
  [irods user path]
  (->> [path ::permission-for user]
       (cache/cached-or-agent (:cache irods) #(permission-for* irods user path) (:jargon-pool irods))))

(defn cached-permission-for
  [irods user path]
  (->> [path ::permission-for user]
       (cache/cached-or-nil (:cache irods))))

(defn maybe-permission-for
  [irods user path]
  (delay (deref (permission-for irods user path))))
