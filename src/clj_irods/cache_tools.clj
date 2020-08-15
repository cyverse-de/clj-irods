(ns clj-irods.cache-tools
  (:require [slingshot.slingshot :refer [try+ throw+]]
            [clojure.tools.logging :as log] 
            ))

(defn rethrow-if-error
  "Takes a return value. If it is an error as created by `do-or-error`,
  re-throws that error."
  [ret]
  (if (::error ret)
    (throw+ (::error ret))
    ret))

(defn- do-or-error
  "Takes an action and tries to execute it. If it throws an error, returns an
  object with the error at a known key."
  [action]
  (log/info "do-or-error" action)
  (try+
    (action)
    (catch Object o
      {::error o})))

(defn- assoc-in-empty
  [m ks v]
  (log/info "assoc-in-empty")
  (when-not (get-in m ks)
    (log/info "assoc'ing")
    (assoc-in m ks v)))

(defn- do-and-store
  "Takes a cache, action, and location in the cache. Puts a `delay` into the
  cache at that location which will execute `do-or-error` with the provided
  action."
  [cache action ks]
  (log/info "do-and-store" action ks)
  (let [store (delay (do-or-error action))]
    (-> cache
        (swap! assoc-in-empty ks store)
        (get-in ks))))

(defn cached-or-do
  "Takes a cache, action, and location in the cache. If the location in the
  cache has something, returns it, otherwise calls `do-or-store` to populate it
  (with a `delay`). Returns the resulting delay, either way."
  [cache action ks]
  (or (get-in @cache ks)
      (do-and-store cache action ks)))

(defn cached-or-agent
  "Takes a cache, action, thread pool, and location in the cache. If the
  location in the cache has something, return it, deref'd, wrapped in
  `rethrow-if-error` and a delay, otherwise initiate an agent and run `action`
  in the specified `pool`. `action` should return something that can be
  deref'd, and will be."
  [cache action pool ks]
  (let [cached (get-in @cache ks)]
    (if (and (delay? cached) (realized? cached))
      (delay (rethrow-if-error @cached))
      (let [ag (agent nil)]
        (send-via pool ag (fn [n] @(action)))
        (delay (await ag) (rethrow-if-error @ag))))))
