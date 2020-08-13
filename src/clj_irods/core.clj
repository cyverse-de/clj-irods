(ns clj-irods.core
  (:require [clj-jargon.init :as init]))

(defmacro with-irods
  "Open connections to iRODS and/or the ICAT depending on the options passed in `cfg`.
   Bind this to the symbol passed in as `sym`. Recommended choice is to call it irods."
  [cfg sym & body]
  (let [jargon-cfg (:jargon cfg)]
    `(init/with-jargon jargon-cfg :lazy true [cm#]
       (let [~sym {:cm cm#}]
         (do ~@body)))))
