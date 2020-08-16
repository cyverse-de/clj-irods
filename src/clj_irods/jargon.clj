(ns clj-irods.jargon
  (:require [slingshot.slingshot :refer [try+]]
            [clj-jargon.item-info :as info]
            [clj-irods.cache-tools :as cache])
  (:import [org.irods.jargon.core.exception FileNotFoundException]))

(defn delay-get-in-fn
  "Creates a function that executes `delay-fn` immediately with args, it should
  return a ref. Return a delay that derefs that and calls `get-in` with `ks` on
  the result."
  [delay-fn ks]
  (fn [& args]
    (let [r (apply delay-fn args)]
      (delay (get-in @r ks)))))

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

;; maybe keep, maybe not (allow only at higher level), idk
(def ^{:arglists '([irods path])} object-type "Get the type of the file at `path`. Returns delay of :file, :dir, or nil." (delay-get-in-fn stat [:type]))
(def ^{:arglists '([irods path])} date-modified (delay-get-in-fn stat [:date-modified]))
(def ^{:arglists '([irods path])} date-created (delay-get-in-fn stat [:date-created]))
(def ^{:arglists '([irods path])} file-size (delay-get-in-fn stat [:file-size]))
(def ^{:arglists '([irods path])} checksum (delay-get-in-fn stat [:md5]))

(defn exists?
  [irods path]
  (delay
    (try+
      (boolean @(object-type irods path))
      (catch FileNotFoundException _ false))))

(defn is-file?
  [irods path]
  (delay (= @(object-type irods path) :file)))

(defn is-dir?
  [irods path]
  (delay (= @(object-type irods path) :dir)))

(def ^{:arglists '([irods path])} maybe-object-type "Get the type of the file at `path`. Returns delay of :file, :dir, or nil." (delay-get-in-fn maybe-stat [:type]))
(def ^{:arglists '([irods path])} maybe-date-modified (delay-get-in-fn maybe-stat [:date-modified]))
(def ^{:arglists '([irods path])} maybe-date-created (delay-get-in-fn maybe-stat [:date-created]))
(def ^{:arglists '([irods path])} maybe-file-size (delay-get-in-fn maybe-stat [:file-size]))
(def ^{:arglists '([irods path])} maybe-checksum (delay-get-in-fn maybe-stat [:md5]))

(defn maybe-exists?
  [irods path]
  (delay
    (try+
      (boolean @(maybe-object-type irods path))
      (catch FileNotFoundException _ false))))

(defn maybe-is-file?
  [irods path]
  (delay (= @(maybe-object-type irods path) :file)))

(defn maybe-is-dir?
  [irods path]
  (delay (= @(maybe-object-type irods path) :dir)))
