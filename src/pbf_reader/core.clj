(ns pbf-reader.core
  (:use clj-configurator.core))

(defconfig settings
  :defaults {:pbf-dir "resources"})

(def files (file-seq (clojure.java.io/file (:pbf-dir settings))))

(defn -main []
  (prn settings)
  (prn (:pbf-dir settings))
  (prn files))
