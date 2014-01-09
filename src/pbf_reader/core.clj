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

(defn- parse-sequence [stream]
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

(defn- parse-stream [pbf-stream]
  (loop [sequence (parse-sequence pbf-stream)]
    (when sequence
      (let [blob-header (:blob-header sequence)
            blob (:blob sequence)
            type (:type blob-header)
            block (blob->block blob type)
            granularity (:granularity block)
            lat_offset (:lat_offset block)
            lon_offset (:lon_offset block)
            date_granularity (:date_granularity block)
            strings (string-table->strings (:s (:stringtable block)))
            primitive-groups (:primitivegroup block)]
        (doseq [group primitive-groups]
          (when-let [dense-nodes (:dense group)]
            (loop [dense-nodes dense-nodes
                   base {:id 0 :lat 0 :lon 0 :ts 0 :cs 0 :uid 0 :sid 0}
                   init-nodes []]
              (let [[diff-id  & rest-ids] (:id  dense-nodes)
                    [diff-lat & rest-lat] (:lat dense-nodes)
                    [diff-lon & rest-lon] (:lon dense-nodes)
                    keys-vals (:keys_vals dense-nodes)
                    id  (+ (:id  base) diff-id)
                    lat (+ (:lat base) diff-lat)
                    lon (+ (:lon base) diff-lon)
                    [tags keys-vals-rest] (decode-dense-tags keys-vals strings)
                    denseinfo (:denseinfo dense-nodes)
                    [version  & rest-versions] (:version   denseinfo)
                    [diff-ts  & rest-ts]       (:timestamp denseinfo)
                    [diff-cs  & rest-cs]       (:changeset denseinfo)
                    [diff-uid & rest-uid]      (:uid       denseinfo)
                    [diff-sid & rest-sid]      (:user_sid  denseinfo)
                    ts  (+ (:ts  base) diff-ts)
                    cs  (+ (:cs  base) diff-cs)
                    uid (+ (:uid base) diff-uid)
                    sid (+ (:sid base) diff-sid)
                    node {:id id :lat (decode-lat-lon lat lat_offset granularity)
                          :lon (decode-lat-lon lon lon_offset granularity)
                          :tags tags :version version :timestamp ts :changeset cs :uid uid :user (get strings sid)}
                    nodes (conj init-nodes node)]
                (if rest-ids
                  (recur {:id rest-ids :lat rest-lat :lon rest-lon :keys_vals keys-vals-rest
                          :denseinfo {:version rest-versions :timestamp rest-ts :changeset rest-cs :uid rest-uid :user_sid rest-sid}}
                         {:id id :lat lat :lon lon :ts ts :cs cs :uid uid :sid sid}
                         nodes)
                  nodes))))
          (when-let [nodes (:nodes group)]
            (throw (Exception. "TODO: Nodes")))
          (when-let [ways (:ways group)]
            (doseq [way ways]
              (let [info (:info way)
                    unpacked-way {:id (:id way) :tags (decode-tags (:keys way) (:vals way) strings) :version (:version info)
                                  :timestamp (:timestamp info) :changeset (:changeset info) :uid (:uid info)
                                  :user (get strings (:user-sid info)) :refs (decode-refs (:refs way))}])))
          (when-let [relations (:relations group)]
            (doseq [rel relations]
              (let [info (:info rel)
                    unpacked-rel {:id (:id rel) :tags (decode-tags (:keys rel) (:vals rel) strings) :version (:version info)
                                  :timestamp (:timestamp info) :changeset (:changeset info) :uid (:uid info)
                                  :user (get strings (:user-sid info)) :refs (decode-rel-refs (:roles-sid rel) (:memids rel) (:types rel) strings)}])))
          (when-let [changesets (:changesets group)]
            (throw (Exception. "TODO: changesets")))))
      (recur (parse-sequence pbf-stream)))))

(defn -main []
  (doseq [pbf pbf-files]
    (with-open [pbf-stream (input-stream pbf)]
      (parse-stream pbf-stream))))
