(ns baldr.core
  (:require [clojure.java.io :refer (file output-stream)])
  (:import [java.nio ByteBuffer ByteOrder]))

(def record-length-buffer-size 8)
(def chunk-length-bytes        1024)

(defn long-from-bytes [bytes]
  (.getLong (.order (ByteBuffer/wrap bytes)
                    ByteOrder/BIG_ENDIAN)))

(defn bytes-from-long [val]
  (let [bytes (byte-array 8)]
    (doto (ByteBuffer/wrap bytes)
      (.order ByteOrder/BIG_ENDIAN)
      (.putLong val))
    bytes))

(defn full-read [istream ^bytes bytes]
  (let [total-record-length (alength bytes)
        chunk-bytes         (byte-array chunk-length-bytes)]
    (loop [total-bytes-read 0]
      (let [bytes-to-read (min chunk-length-bytes (- total-record-length total-bytes-read))
            bytes-read    (.read istream chunk-bytes 0 bytes-to-read)]
        (if (> bytes-read 0)
          (do (System/arraycopy chunk-bytes 0
                                bytes       total-bytes-read
                                bytes-read)
              (recur (+ total-bytes-read bytes-read)))
          total-bytes-read)))))

(defn read-record [istream]
  (let [record-length-bytes (byte-array record-length-buffer-size)
        bytes-read          (full-read istream record-length-bytes)]
    (when (> bytes-read 0)
      (let [record-length (long-from-bytes record-length-bytes)
            record        (byte-array record-length)]
        (full-read istream record)
        record))))

(defn baldr-seq
  [istream]
  (take-while (comp not nil?) (repeatedly (partial read-record istream))))
