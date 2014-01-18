(ns pbf-reader.core
  (:import (crosby.binary Osmformat$HeaderBlock Fileformat$BlobHeader Fileformat$Blob Osmformat$PrimitiveBlock)
           (java.util.zip Inflater))
  (:use [clojure.java.io :only [input-stream]])
  (:require [flatland.protobuf.core :as buf]))

(def BlobHeader     (buf/protodef Fileformat$BlobHeader))
(def Blob           (buf/protodef Fileformat$Blob))
(def HeaderBlock    (buf/protodef Osmformat$HeaderBlock))
(def PrimitiveBlock (buf/protodef Osmformat$PrimitiveBlock))

;; All credit for this function goes to pingles
(defn- bytes->int [bytes]
  (reduce + 0 (map #(bit-shift-left (bit-and (nth bytes %)
                                             0xFF)
                                    (* (- 4 1 %) 8))
                   (range 0 4))))

(defn- fill-bytes! [stream bytes]
  (let [count-bytes (count bytes)
        read (.read stream bytes 0 count-bytes)]
    (= count-bytes read)))

(defn- get-bytes [stream num-of-bytes]
  (let [bytes (byte-array num-of-bytes)]
    (when (fill-bytes! stream bytes)
      bytes)))

(defn- decode-chunk [stream]
  ;; TODO: check size of blobs
  (when-let       [header-size  (bytes->int (get-bytes stream 4))]
    (when-let     [header-bytes (get-bytes stream header-size)]
      (let        [blob-header  (buf/protobuf-load BlobHeader header-bytes)]
        (when-let [blob-bytes   (get-bytes stream (:datasize blob-header))]
          {:blob-header blob-header :blob (buf/protobuf-load Blob blob-bytes)})))))

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
    (reduce #(conj % (.toStringUtf8 %2)) [] byte-strings)))

(defn- calculate-lat-lon [value offset granularity]
  (* 0.000000001 (+ offset (* granularity value))))

(defn decode-dense-tags [keys-vals strings]
  (loop [[k v & keys-vals] keys-vals
         tags '()]
    (cond
      (and k (> k 0)) (recur keys-vals (cons [(strings k) (strings v)] tags))
      k (recur (cons v keys-vals) (cons [] tags))
      :else tags)))

(defn- decode-tags [keys vals strings]
  ;; which is faster?
  #_(into {} (map #(vector % %2) (map strings keys) (map strings vals)))
  (apply hash-map (interleave (map strings keys) (map strings vals))))

(defn- decode-refs [refs]
  (:decoded (reduce #(let [ref (+ (:base %) %2)]
                      {:base ref :decoded (conj (:decoded %) ref)})
                    {:base 0 :decoded []} refs)))

(defn- decode-delta [s]
  (reduce #(conj % (+ %2 (peek %)))
          (vector (first s))
          (rest s)))

(defn- decode-rel-refs [role-sids member-ids types strings]
  (map #(hash-map :role (strings %) :id %2 :type %3)
       role-sids
       (decode-delta member-ids)
       types))

(defn- parse-dense [dense strings]
  (let [{:keys [denseinfo keys-vals]} dense
        ids (decode-delta (:id dense))
        users (map #(strings %) (decode-delta (:user-sid denseinfo)))
        versions (:version denseinfo)
        tags (decode-dense-tags keys-vals strings)
        lats (decode-delta (:lat dense))
        lons (decode-delta (:lon dense))
        changesets (decode-delta (:changeset denseinfo))
        timestamps (decode-delta (:timestamp denseinfo))
        uids (decode-delta (:uid denseinfo))]
    {:nodes (apply map #(hash-map :id % :user %2 :tags %3 :version %4 :changeset %5 :lat %6 :lon %7 :timestamp %8 :uid %9)
                   [ids users tags versions changesets lats lons timestamps uids])}))

(defn- parse-info [info strings]
  (let [{:keys [version timestamp changeset uid user-sid]} info]
    {:user (strings user-sid) :version version :changeset changeset :timestamp timestamp :uid uid}))

(defn parse-ways [ways strings]
  (let [parse-way (fn [way]
                    (let [{:keys [id keys vals refs info]} way
                          refs (decode-delta refs)]
                      (into {} (concat (parse-info info strings)
                                       {:id id :tags (decode-tags keys vals strings) :refs refs}))))]
    {:ways (doall (map parse-way ways))}))

(defn- parse-relations [relations strings]
  (let [parse-relation (fn [rel]
                         (let [{:keys [id keys vals info roles-sid memids types]} rel]
                           (into {} (concat (parse-info info strings)
                                            {:id id :tags (decode-tags keys vals strings)
                                             :refs (decode-rel-refs roles-sid memids types strings)}))))]
    {:relations (doall (map parse-relation relations))}))

(defn- parse-group [group block]
  (let [{:keys [granularity lat-offset lon-offset date-granularity primitivegroup]} block
        {:keys [dense nodes ways relations changesets]} group
        strings (string-table->strings (:s (:stringtable block)))]
    (cond
      dense (parse-dense dense strings)
      ways (parse-ways ways strings)
      relations (parse-relations relations strings))))

(defn- parse-chunk [chunk]
  (let [{:keys [blob-header blob]} chunk
        type (:type blob-header)
        block (blob->block blob type)
        {:keys [primitivegroup]} block]
    (when primitivegroup
      (loop [[group & groups] primitivegroup
             decoded '()]
        (if group
          (recur groups (conj decoded (parse-group group block)))
          decoded)))))

(defn decode-pbf [stream]
  (let [chunk (decode-chunk stream)]
    (when-not (empty? chunk)
      (if-let [chunk (parse-chunk chunk)]
        (cons chunk
              (lazy-seq (decode-pbf stream)))
        (decode-pbf stream)))))
