(ns clj-irods.validate
  (:require [clojure-commons.error-codes :as error]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]
            [otel.otel :as otel]
            [clj-irods.core :as rods]))

(defn- to-sequence
  "Returns a sequence of values for the given argument. If the argument is sequential then it's simply returned.
  Otherwise, a single-element vector containing the argument is returned."
  [v]
  (if (sequential? v) v [v]))

(defn validate
  "Validate a set of things in iRODS.

  Each validation is a vector of a keyword (identifying the kind of validation)
  and any relevant arguments. The validations will be run in the provided order.

  Validations that are equal to nil will be ignored. This makes it easier for callers to add
  conditional validations as in:

      (validate irods
                [:user-exists user zone]
                (when validate-path? [:path-exists path user zone]))

  Available validations and their arguments:

  :user-exists (string or vector, users to check), (string zone)
  :path-exists (string or vector, path or paths to check), (string user), (string zone)
  :path-is-file (string or vector, path or paths to check), (string user), (string zone)
  :path-is-dir (string or vector, path or paths to check), (string user), (string zone)
  :path-readable (string or vector, path or paths to check), (string user), (string zone)
  :path-writeable (string or vector, path or paths to check), (string user), (string zone)
  :path-owned (string or vector, path or paths to check), (string user), (string zone)
  :uuid-exists (string or vector, uuid or uuids to check)
  "
  [irods & validations]

  (otel/with-span [s ["validate"]]
    (doseq [v (remove nil? validations)]
      (condp = (first v)
        :user-exists (let [[users zone] (rest v)
                           user-missing? (fn [u] (or (nil? u) (= @(rods/user-type irods u zone) :none)))]
                       (when-let [missing-users (seq (filter user-missing? (to-sequence users)))]
                         (throw+ {:error_code error/ERR_NOT_A_USER
                                  :users missing-users})))
        :path-exists (let [[paths user zone] (rest v)
                           path-missing? (fn [p] (or (nil? p) (= @(rods/object-type irods user zone p) :none)))]
                       (when-let [missing-paths (seq (filter path-missing? (to-sequence paths)))]
                         (throw+ {:error_code error/ERR_DOES_NOT_EXIST
                                  :paths missing-paths})))
        :path-not-exists (let [[paths user zone] (rest v)
                               path-exists? (fn [p] (not= @(rods/object-type irods user zone p) :none))]
                           (when-let [extant-paths (seq (filter path-exists? (to-sequence paths)))]
                             (throw+ {:error_code error/ERR_EXISTS
                                      :paths extant-paths})))
        :path-is-file (let [[paths user zone] (rest v)
                            path-not-file? (fn [p] (not= @(rods/object-type irods user zone p) :file))]
                        (when-let [invalid-paths (seq (filter path-not-file? (to-sequence paths)))]
                          (throw+ {:error_code error/ERR_NOT_A_FILE
                                   :paths invalid-paths})))
        :path-is-dir (let [[paths user zone] (rest v)
                           path-not-dir? (fn [p] (not= @(rods/object-type irods user zone p) :dir))]
                       (when-let [invalid-paths (seq (filter path-not-dir? (to-sequence paths)))]
                         (throw+ {:error_code error/ERR_NOT_A_FOLDER
                                  :paths invalid-paths})))
        :path-readable (let [[paths user zone] (rest v)
                             readable? (fn [p] (contains? #{:read :write :own} @(rods/permission irods user zone p)))]
                         (when-let [unreadable-paths (seq (remove readable? (to-sequence paths)))]
                           (throw+ {:error_code error/ERR_NOT_READABLE
                                    :paths unreadable-paths
                                    :user user})))
        :path-writeable (let [[paths user zone] (rest v)
                              writeable? (fn [p] (contains? #{:write :own} @(rods/permission irods user zone p)))]
                          (when-let [unwriteable-paths (seq (remove writeable? (to-sequence paths)))]
                            (throw+ {:error_code error/ERR_NOT_WRITEABLE
                                     :paths unwriteable-paths
                                     :user user})))
        :path-owned (let [[paths user zone] (rest v)
                          owner? (fn [p] (= :own @(rods/permission irods user zone p)))]
                      (when-let [unowned-paths (seq (remove owner? (to-sequence paths)))]
                        (throw+ {:error_code error/ERR_NOT_OWNER
                                 :paths unowned-paths
                                 :user user})))
        :uuid-exists (let [[uuids] (rest v)
                           uuids (to-sequence uuids)
                           path-map @(rods/uuids->paths irods uuids)]
                       (when-let [missing-uuids (seq (remove path-map uuids))]
                         (throw+ {:error_code error/ERR_DOES_NOT_EXIST
                                  :ids missing-uuids})))
        (log/warn "Unrecognized validation type:" (first v))))))
