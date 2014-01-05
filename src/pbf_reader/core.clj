(ns pbf-reader.core
  (:use [clojure.java.io :only [input-stream]])
  (:require [clj-configurator.core :as conf]))

(conf/defconfig settings
  :defaults {:pbf-dir "resources"})

(def pbf-files
  (filter
    #(re-find #"\.osm\.pbf$" (.getName %))
    (file-seq (clojure.java.io/file (:pbf-dir settings)))))

(defn -main []
  (doseq [pbf pbf-files]
    (with-open [file-stream (input-stream pbf)]
      (prn file-stream))))
