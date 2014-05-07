(ns baldr.core-test
  (:require [clojure.test :refer :all]
            [baldr.core :refer :all])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))


(deftest read-record-test
  (let [header    (bytes-from-long 3)
        record    (byte-array (range 3))
        raw-bytes (byte-array (concat header record))]
    (is (= (seq (byte-array (range 3)))
           (seq (read-record (ByteArrayInputStream. raw-bytes)))))))

(deftest multi-chunk-record-test
  (let [n         1200
        header    (bytes-from-long n)
        record    (byte-array (range n))
        raw-bytes (byte-array (concat header record))]
    (is (= (seq (byte-array (range n)))
           (seq (read-record (ByteArrayInputStream. raw-bytes)))))))

(defn record [n]
  (let [header (bytes-from-long n)
        record (byte-array (range n))]
    (byte-array (concat header record))))

(deftest multi-record-test
  (let [records (byte-array (concat (record 100) (record 200)))
        bin     (ByteArrayInputStream. records)
        record1 (read-record bin)
        record2 (read-record bin)
        record3 (read-record bin)]
    (is (= (seq (byte-array (range 100)))
           (seq record1)))
    (is (= (seq (byte-array (range 200)))
           (seq record2)))
    (is (nil? record3))))

(deftest record-sequence
  (let [records     (byte-array (concat (record 100) (record 200) (record 300)))
        bin         (ByteArrayInputStream. records)
        record-seq  (baldr-seq bin)]
    (is (= 3 (count record-seq)))
    (is (= (seq (byte-array (range 100)))
           (seq (first record-seq))))
    (is (= (seq (byte-array (range 300)))
           (seq (last record-seq))))))


(deftest stream-roundtrip
  (let [ostream (ByteArrayOutputStream. 100)
        write (baldr-writer ostream)]
    (write (byte-array [1 2 3]))
    (write (byte-array [1 2 3 4]))
    (.close ostream)

    (let [records (baldr-seq (ByteArrayInputStream. (.toByteArray ostream)))]
      (is (= (seq (byte-array [1 2 3]))
             (seq (first records))))
      (is (= (seq (byte-array [1 2 3 4]))
             (seq (last records)))))))
