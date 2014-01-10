(ns pbf-reader.core
  (:import (crosby.binary Osmformat$HeaderBlock Fileformat$BlobHeader Fileformat$Blob Osmformat$PrimitiveBlock)
           (java.util.zip Inflater))
  (:use [clojure.java.io :only [input-stream]]
        [clj-configurator.core :only [defconfig]])
  (:require [flatland.protobuf.core :as buf]))

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

(defn- parse-header [stream]
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

(defn- string-table->strings [byte-strings]
  (when byte-strings
    (loop [[first & rest] byte-strings
           init-strings []]
      (let [strings (conj init-strings (.toStringUtf8 first))]
        (if rest
          (recur rest strings)
          strings)))))

(defn- decode-lat-lon [value offset granularity]
  (* 0.000000001 (+ offset (* granularity value))))

;; TODO: rewrite it!
(defn- decode-dense-tags [keys-vals strings]
  (loop [keys {}
         keys-vals keys-vals]
    (if (>= (count keys-vals) 2)
      (let [[key val & rest] keys-vals]
        (if (> key 0)
          (recur (assoc keys (get strings key) (get strings val)) rest)
          [keys (into rest [val])]))
      [keys keys-vals])))

(defn- decode-tags [keys vals strings]
  (loop [tags {}
         ks keys
         vs vals]
    (if ks
      (let [[k & ks-rest] ks
            [v & vs-rest] vs]
        (recur (assoc tags (get strings k) (get strings v)) ks-rest vs-rest))
      tags)))

(defn- decode-refs [refs]
  (loop [decoded-refs []
         base 0
         rs refs]
    (if rs
      (let [[r & rs-rest] rs
            ref (+ base r)]
        (recur (conj decoded-refs ref) ref rs-rest))
      decoded-refs)))

(defn- decode-rel-refs [role-sids member-ids types strings]
  (loop [r-sids role-sids
         m-ids member-ids
         ts types
         base-m-id 0
         refs []]
    (if ts
      (let [[r & rs-rest] r-sids
            [m & ms-rest] m-ids
            [t & ts-rest] ts
            m-id (+ base-m-id m)]
        (recur rs-rest ms-rest ts-rest m-id (conj refs {:role (get strings r) :type t :id m-id})))
      refs)))

(defn- encode-delta [s]
  (reverse (reduce #(if (seq? %) (cons (+ %2 (first %)) %)
                                 (cons (+ %2 %) [%])) s)))

(defn- parse-dense [dense strings]
  (let [{:keys [denseinfo keys-vals]} dense
        ids (encode-delta (:id dense))
        users (map #(strings %) (encode-delta (:user-sid denseinfo)))
        versions (:version denseinfo)
        tags (decode-dense-tags keys-vals strings)
        lats () ;; TODO
        lons () ;; TODO
        changesets (encode-delta (:changeset denseinfo))
        timestamps (encode-delta (:timestamp denseinfo))
        uids (encode-delta (:uid denseinfo))]
    {:nodes (apply map #(hash-map :id % :user %2 :version %3 :tags %4 :changeset %5 :lat %6 :lon %7 :timestamp %8 :uid %9)
                   [ids users versions tags changesets lats lons timestamps uids])}))

(defn- parse-group [group block]
  (let [{:keys [granularity lat-offset lon-offset date-granularity primitivegroup]} block
        {:keys [dense nodes ways relationschangesets]} group
        strings (string-table->strings (:s (:stringtable block)))]
    (cond
      dense (parse-dense dense strings))))

(defn- parse-stream [stream]
  (loop [chunk (parse-header stream)]
    (when chunk
      (let [{:keys [blob-header blob]} chunk
            type (:type blob-header)
            block (blob->block blob type)
            {:keys [primitivegroup]} block]
        (when primitivegroup
          (loop [[group & groups] primitivegroup]
            (when group
              (conj (parse-group group block) #_(recur groups)))))
        (recur (parse-header stream))))))

(defn -main []
  (when pbf-files
    (loop [[file & rest-files] pbf-files]
      (with-open [stream (input-stream file)]
        (parse-stream stream))
      (when rest-files
        (recur rest-files)))))
