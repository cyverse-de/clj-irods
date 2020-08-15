(ns clj-irods.cache-tools
  (:require [slingshot.slingshot :refer [try+ throw+]]))

(defn- do-or-error
  "Takes an action and tries to execute it. If it throws an error, returns an
  object with the error at a known key."
  [action]
  (try+
    (action)
    (catch Object o
      {::error o})))

(defn- do-and-store
  "Takes a cache, action, and location in the cache. Puts a `delay` into the
  cache at that location which will execute `do-or-error` with the provided
  action."
  [cache action ks]
  (let [store (delay (do-or-error action))]
    (-> cache
        (swap! assoc-in ks store)
        (get-in ks))))

(defn cached-or-do
  "Takes a cache, action, and location in the cache. If the location in the
  cache has something, returns it, otherwise calls `do-or-store` to populate it
  (with a `delay`)."
  [cache action ks]
  (or (get-in @cache ks)
      (do-and-store cache action ks)))

(defn rethrow-if-error
  "Takes a return value. If it is an error as created by `do-or-error`,
  re-throws that error."
  [ret]
  (if (::error ret)
    (throw+ (::error ret))
    ret))

(defn get-cached
  [cache ks]
  (get-in @cache ks))

