(ns baldr.core
  (:require [clojure.java.io :refer (file output-stream)])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.io InputStream OutputStream]))

(def record-length-buffer-size 8)
(def chunk-length-bytes        1024)

(defn- long-from-bytes
  [^bytes bytes]
  (.getLong (.order (ByteBuffer/wrap bytes)
                    ByteOrder/BIG_ENDIAN)))

(defn- bytes-from-long
  [val]
  (let [bytes (byte-array 8)]
    (doto (ByteBuffer/wrap bytes)
      (.order ByteOrder/BIG_ENDIAN)
      (.putLong val))
    bytes))

(defn- full-read
  [^InputStream istream ^bytes bytes]
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


;; +------------------------+
;; |         Record         |
;; +------------------------+  ....
;; | header     | payload   |
;; +------------+-----------+  ....
;; |  8 bytes   |  n bytes  |
;; +------------+-----------+  ....
;;
;; header contains the length of the payload

(defn- read-record
  [istream]
  (let [record-length-bytes (byte-array record-length-buffer-size)
        bytes-read          (full-read istream record-length-bytes)]
    (when (> bytes-read 0)
      (let [record-length (long-from-bytes record-length-bytes)
            record        (byte-array record-length)]
        (full-read istream record)
        record))))

(defn baldr-seq
  "Consumes bytes from istream and returns a seq of byte-arrays
  corresponding to the baldr records they correspond to. Does not handle
  corrupted bytestreams."
  [^InputStream istream]
  (take-while (comp not nil?) (repeatedly (partial read-record istream))))

(defn baldr-record
  "Returns a new byte-array containing the length of payload, followed
  by payload."
  [^bytes payload]
  (let [payload-length (alength payload)
        record         (byte-array (+ record-length-buffer-size payload-length))
        header         (bytes-from-long payload-length)]
    (System/arraycopy header  0
                      record  0
                      record-length-buffer-size)
    (System/arraycopy payload 0
                      record  record-length-buffer-size
                      payload-length)
    record))

(defn baldr-writer
  "Returns a function which takes byte-arrays and writes them to ostream
  as baldr-records, containing the byte-array lengths and their
  contents."
  [^OutputStream ostream]
  (fn [^bytes payload]
    (.write ostream ^bytes (baldr-record payload))))
