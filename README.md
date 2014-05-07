![Baldr](http://upload.wikimedia.org/wikipedia/commons/e/ea/Each_arrow_overshot_his_head_by_Elmer_Boyd_Smith.jpg)

# baldr

Named after the "god of light and purity" in Norse mythology. A light a pure file format for records of bytes.

## Usage

Add the library to your project dependencies.

    [baldr "0.1.1"]

Write byte-arrays to an `OutputStream` using `baldr-writer`.

```clojure
(let [ostream (ByteArrayOutputStream. 100)
      write (baldr.core/baldr-writer ostream)]
  (write (byte-array [1 2 3]))
  (write (byte-array [1 2 3 4]))
  (.close ostream))
```

Read seq of byte-arrays from an `InputStream` using `baldr-seq`.

```clojure
(defn record [n]
  (let [header (bytes-from-long n)
        record (byte-array (range n))]
    (byte-array (concat header record))))

(let [ostream (ByteArrayOutputStream. 100)
      write (baldr-writer ostream)]
  (write (byte-array [1 2 3]))
  (write (byte-array [1 2 3 4]))
  (.close ostream)
  (baldr-seq (ByteArrayInputStream. (.toByteArray ostream))))
```

## License

Copyright © uSwitch

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
