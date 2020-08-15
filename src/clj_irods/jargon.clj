(ns clj-irods.jargon
  (:require [clj-jargon.item-info :as info]
            [clj-irods.cache-tools :as cache]))

;; This namespace is probably mostly going to be structured as pairs of
;; functions, one using cached-or-do and calling to jargon (creating a delay
;; that does the actual work), and the other using cached-or-agent and calling
;; the first function, that handles running things in appropriate thread pools
;; and initiates actual realization of values.

;; Cache keys should be by path or ID followed by a namespaced keyword
;; (::whatever) so it's clear it corresponds to this namespace.

(defn stat*
  "Creates and caches a delay for a stat of the path."
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
