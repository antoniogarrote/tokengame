(ns tokengame.examples.wordcount
  (:use [tokengame.petri]))

(defn mapper
  ([line]
     (let [words (vec (.split line " "))
           pairs (reduce (fn [h w] (if (nil? (get h w))
                                     (assoc h w 1)
                                     (assoc h w (inc (get h w)))))
                         {} words)]
       (doseq  [[w c] pairs]
         (fire ["pairs"] [w c])))))

(defn reducer
  ([arg1 arg2]
     (let [wordcount (if (map? arg1) arg1 arg2)
           pair (if (map? arg1) arg2 arg1)
           [w c] pair
           old-count (if (nil? (get wordcount (keyword w))) 0 (get wordcount (keyword w)))
           new-count (+ old-count c)
           new-wordcount (assoc wordcount (keyword w) new-count)]
       (fire ["out"] new-wordcount))))

;; older versions

(def *partitions* (ref {}))

(defn mapper-multi
  ([line]
     (let [words (vec (.split line " "))
           pairs (reduce (fn [h w] (if (nil? (get h w))
                                     (assoc h w 1)
                                     (assoc h w (inc (get h w)))))
                         {} words)]
       (doseq  [[w c] pairs]
         (let [ch (first w)]
           ; a-h
           (if (and (> (int ch) (int \a))
                    (<= (int ch) (int \h)))
             (fire ["words-ah"] (str [w c]))
             ; i-o
             (if (and (> (int ch) (int \h))
                      (<= (int ch) (int \o)))
               (fire ["words-io"] (str [w c]))
               ; p-z
               (fire ["words-oz"] (str [w c])))))))))

(defn add-to-partitions
  ([partitions word-list]
     (dosync
      (let [[w c] word-list
            old-word-list (get @partitions w)]
        (if (nil? old-word-list)
          (alter partitions (fn [old] (assoc old w [c])))
          (alter partitions (fn [old] (assoc old w (conj old-word-list c)))))))))

(defn partitioner
  ([word-list]
     (if (= word-list "end-of-lines")
       @*partitions*
       (do
         (remote-log :info (str "PARTITIONER ADDING: " word-list))
         (add-to-partitions *partitions* word-list)
         (remote-log :info (str "PARTITIONS NOW: " @*partitions*))))))

(defn reducer-multi
  ([word-dict]
     (doseq [k (keys word-dict)]
       (println (str "word: " k " count: " (apply + (get word-dict k)))))))
