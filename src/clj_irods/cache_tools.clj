(ns clj-irods.cache-tools
  "This namespace contains functions for manipulating a cache stored in an
  atom, using agents to calculate real values. Objects are identified by keys
  that are paths into an object, compatible with get-in, assoc-in, etc.

  Consumers of this namespace will mostly use three functions:

  cached-or-do: this function is best used in a private function, and the
  action passed to it should be the actual, final calculation of the value. It
  will usually run inside an agent.

  cached-or-agent: this function is best used for the main public API, and the
  action passed to it should be the private function above that calls
  `cached-or-do`. It will run what it is passed in an agent (in the passed-in
  pool).

  cached-or-nil: this function is best used for a secondary API used to fetch
  results, but only if they're already in the cache. It takes only the cache
  and a key, since it does no calculation of values. It will return a
  deref-able thing (a delay, generally) when there is a cached value, or nil
  when not, so its return values play nicely with `if` and `when`.

  Because this namespace is designed to work with agents, it uses some plumbing
  for errors. Ordinarily, when an exception is thrown in an agent, the agent
  hangs until `agent-error` is called on it. Instead, this namespace puts it
  into a namespaced keyword (::error), and provides a function
  `rethrow-if-error` that will rethrow the error from that key if it's present
  in a returned value. Both `cached-or-agent` and `cached-or-nil` use this
  function themselves, but if the cache is used directly outside this namespace
  the function might need to be used there."
  (:require [slingshot.slingshot :refer [try+ throw+]]
            [medley.core :refer [dissoc-in map-kv-vals]]
            [otel.otel :as otel]
            [clojure.set :as set]
            [clojure.tools.logging :as log]))

(defmacro otel-with-subspan [[s] & body]
  `(with-open [_# (otel/span-scope ~s)]
     ~@body))

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
  [action & args]
  (try+
    (apply action args)
    (catch Object o
      {::error o})))

(defn- assoc-in-empty
  [m ks v]
  (when-not (get-in m ks)
    (assoc-in m ks v)))

(defn- do-and-store
  "Takes a cache, action, and location in the cache. Puts a `delay` into the
  cache at that location which will execute `do-or-error` with the provided
  action."
  [cache action ks]
  (let [store (delay (do-or-error action))]
    (-> cache
        (swap! assoc-in-empty ks store)
        (get-in ks))))

(defn cached-or-do
  "Takes a cache, action, and location in the cache. If the location in the
  cache has something, returns it, otherwise calls `do-and-store` to populate it
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
    (if (delay? cached) ;; here, only check that something's there, deref later is fine
      (do
        (log/info "got cached value:" ks)
        (delay (rethrow-if-error @cached)))
      (otel/with-span [s ["agent for calculation"]]
        (log/info "launching agent:" ks)
        (let [ag (agent nil)]
          (send-via pool ag (fn [_nil] (otel-with-subspan [s] @(action))))
          (delay (await ag) (rethrow-if-error @ag)))))))

(defn cached-or-nil
  "Takes a cache and location in the cache. If the location in the cache has
  something and it's a realized delay, return it wrapped in rethrow-if-error
  and a new delay. Otherwise, return nil (*not* in a delay, for clarity)."
  [cache ks]
  (let [cached (get-in @cache ks)]
    (when (and (delay? cached) (realized? cached)) ;; here, only allow realized delays, because we want to never do actual calculation
      (log/info "got cached value:" ks)
      (delay (rethrow-if-error @cached)))))

(defn- get-cached-values
  [cache location-fn ids]
  (->> (mapv (juxt identity (comp (partial get-in @cache) location-fn)) ids)
       (filter (comp delay? second))
       (map (fn [[id cached]] [id (delay (rethrow-if-error @cached))]))
       (into {})))

(defn- store-multi
  [cache location-fn ids ag]
  (->> (for [id ids]
         (let [ks          (location-fn id)
               cache-entry (delay (await ag) (get (rethrow-if-error @ag) id))]
           [id (get-in (swap! cache assoc-in-empty ks cache-entry) ks)]))
       (into {})))

(defn cached-or-retrieved-values
  [cache action pool location-fn ids]
  (let [cached       (get-cached-values cache location-fn ids)
        uncached-ids (set/difference (set ids) (set (keys cached)))
        deref-vals   (fn [m] (map-kv-vals (fn [_ v] @v) m))]
    (if-not (empty? uncached-ids)
      (otel/with-span [s ["agent for retrieving multiple values"]]
        (let [ag (agent nil)]
          (send-via pool ag (fn [_nil] (otel-with-subspan [s] (do-or-error action uncached-ids))))
          (let [stored-delays (store-multi cache location-fn ids ag)]
            (delay (deref-vals (merge cached stored-delays)))))
      (delay (deref-vals cached)))))

(defn clear-cache-prefix
  "Takes a cache and a cache prefix, clearing that prefix."
  [cache ks]
  (swap! cache dissoc-in ks))
