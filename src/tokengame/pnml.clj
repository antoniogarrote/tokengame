(ns tokengame.pnml
  (:use [clojure.xml]))

(defn add-arc
  ([components arc-tag]
     (let [id (:id (:attrs arc-tag))
           source (:source (:attrs arc-tag))
           target (:target (:attrs arc-tag))
           old-arcs (:arcs components)]
       (assoc components :arcs (conj old-arcs {:id id :source source :target target})))))

(defn add-place
  ([components place-tag]
     (let [id (:id (:attrs place-tag))
           name (first (:content (first (:content (first (:content place-tag))))))
           old-places (:places components)]
       (assoc components :places (conj old-places {:id id :name name})))))

(defn add-transition
  ([components transition-tag]
     (let [id (:id (:attrs transition-tag))
           name (first (:content (first (:content (first (:content transition-tag))))))
           old-transitions (:transitions components)]
       (assoc components :transitions (conj old-transitions {:id id :name name})))))

(defn find-components
  ([elements components]
     (if (empty? elements)
       components
       (let [element (first elements)]
         (condp = (:tag element)
           :place      (recur (rest elements) (add-place components element))
           :net        (recur (concat (rest elements) (:content element)) components)
           :transition (recur (rest elements) (add-transition components element))
           :arc        (recur (rest elements) (add-arc components element))
           (recur (rest elements) components))))))

(defn make-name-for-transition
  ([name] (if (not= -1 (.indexOf name ":"))
            (aget (.split name ":") 0)
            name)))

(defn make-function-for-transition
  ([name] (if (not= -1 (.indexOf name ":"))
            (aget (.split name ":") 1)
            name)))

(defn make-network-description
  ([arcs places transitions]
     (if (empty? arcs) {:places places :transitions transitions}
         (let [arc (first arcs)
               source (:source arc)
               target (:target arc)
               source-place (first (filter #(= (:id %1) source) places))]
           (if (nil? source-place)
             (let [source-transition (first (filter #(= (:id %1) source) transitions))
                   rest-transitions (filter #(not= (:id %1) source) transitions)
                   target-place (first (filter #(= (:id %1) target) places))
                   old-places-out (:places-out source-transition)
                   transitionp (assoc source-transition :places-out (conj old-places-out target-place))]
               (recur (rest arcs) places (conj rest-transitions transitionp )))
             (let [target-transition (first (filter #(= (:id %1) target) transitions))
                   rest-transitions (filter #(not= (:id %1) target) transitions)
                   old-places-in (:places-in target-transition)
                   transitionp (assoc target-transition :places-in (conj old-places-in source-place))]
               (recur (rest arcs) places (conj rest-transitions transitionp)))))))

  ([components]
     (let [transitions (:transitions components)
           places (:places components)
           arcs   (:arcs components)]
       (make-network-description arcs
                                 (map (fn [p] (-> p
                                                  (assoc :size 1)
                                                  (assoc :model-component :place)))
                                      places)
                                 (reduce (fn [acum t] (conj acum (-> t
                                                                     (assoc :name (make-name-for-transition (:name t)))
                                                                     (assoc :places-in [])
                                                                     (assoc :places-out [])
                                                                     (assoc :function (make-function-for-transition (:name t)))
                                                                     (assoc :model-component :transition))))
                                         [] transitions)))))

(defn parse-pnml
  "Parses a PNML document from a String or a java.io.File"
  ([file-or-doc]
     (let [doc (if (instance? java.io.File file-or-doc) (parse file-or-doc) (parse (java.io.ByteArrayInputStream. (.getBytes file-or-doc))))
           before (make-network-description (find-components (:content doc) {:places [] :transitions [] :arcs []}))
           after {:places (doall (:places before))
                  :transitions (doall (:transitions before))}]
       ;; lazy seq and java serialization problem
       after)))


;; Model manipulation

(defn out-transitions-for-place
  ([place model]
     (filter
      (fn [t] (some #(= (:name %1) (:name place)) (:places-in t)))(:transitions model))))
