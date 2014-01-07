(ns pbf-reader.core
  (:import (crosby.binary Osmformat$HeaderBlock Fileformat$BlobHeader Fileformat$Blob Osmformat$PrimitiveBlock)
           (java.util.zip Inflater))
  (:use [clojure.java.io :only [input-stream]]
        [clj-configurator.core :only [defconfig]])
  (:require
            [flatland.protobuf.core :as buf]))

;; default config
(defconfig settings
  :defaults {:pbf-dir "resources"})

(def BlobHeader     (buf/protodef Fileformat$BlobHeader))
(def Blob           (buf/protodef Fileformat$Blob))
(def HeaderBlock    (buf/protodef Osmformat$HeaderBlock))
(def PrimitiveBlock (buf/protodef Osmformat$PrimitiveBlock))

;; All credit for this function goes to pingles
(defn- bytes->int
  ([bytes]
   (bytes->int bytes 0))
  ([bytes offset]
   (reduce + 0 (map (fn [i]
                      (let [shift (* (- 4 1 i) 8)]
                        (bit-shift-left (bit-and (nth bytes (+ i offset))
                                                 0xFF)
                                        shift)))
                    (range 0 4)))))

(def pbf-files
  (filter
    #(re-find #"\.osm\.pbf$" (.getName %))
    (file-seq (clojure.java.io/file (:pbf-dir settings)))))

(defn- fill-bytes! [stream bytes]
  (let [count-bytes (count bytes)
        read (.read stream bytes 0 count-bytes)]
    (= count-bytes read)))

(defn- get-bytes [stream num-of-bytes]
  (let [bytes (byte-array num-of-bytes)]
    (when (fill-bytes! stream bytes)
      bytes)))

(defn- parse-file [stream]
  ;; TODO: check size of blobs
  (when-let       [header-size (get-bytes stream 4)]
    (when-let     [header      (get-bytes stream (bytes->int header-size))]
      (let        [blob-header (buf/protobuf-load BlobHeader header)]
        (when-let [blob-size   (get-bytes stream (:datasize blob-header))]
          {:blob-header blob-header :blob (buf/protobuf-load Blob blob-size)})))))

(defn- zlib-unpack [input size]
  (let [inflater (Inflater.)
        output   (byte-array size)]
    (.setInput inflater input)
    (.inflate  inflater output)
    output))

(defn- type->blockname [type]
  (cond
    (= type "OSMHeader") HeaderBlock
    (= type "OSMData")   PrimitiveBlock))

(defn- blob->block [blob type]
  (when-let [blockname (type->blockname type)]
    (buf/protobuf-load blockname (zlib-unpack (.toByteArray (:zlib-data blob)) (:raw-size blob)))))

(defn -main []
  (doseq [pbf pbf-files]
    (with-open [pbf-stream  (input-stream pbf)]
      (loop [sequence (parse-file pbf-stream)
             ;; just for tests
             n 1]
        (when sequence
          (let [blob-header (:blob-header sequence)
                blob (:blob sequence)
                type (:type blob-header)
                block (blob->block blob type)]
            ;; just for tests
            (prn blob-header)
            (prn blob)
            (prn block))
          ;; just for tests
          (when (> n 0)
            (recur (parse-file pbf-stream) (- n 1))))))))
