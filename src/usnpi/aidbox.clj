(ns usnpi.aidbox
  (:require [clojure.walk :as walk]
            [clojure.string :as str]))

(defn reference?
  [x]
  (and (map? x)
       (:reference x)))

(defn reference-id?
  [{:keys [reference]}]
  (re-matches #"^(\w+?)/(\w+?)$" reference))

(defn reference-uri?
  [{:keys [reference]}]
  (re-find #"^(?i)(http)|(https)://" reference))

(defn ->reference
  [{:keys [reference] :as x}]
  (cond
    (reference-id? x)
    (let [[resource id] (str/split reference #"/")]
      {:id id :resourceType resource})

    (reference-uri? x)
    {:uri reference}

    :else x))

(defn map-node? [x]
  (and (vector? x)
       (= (count x) 2)))

(defn field-x? [x]
  (re-matches #"^value[A-Z].*" (name x)))

(defn value-x? [x]
  (and (map-node? x)
       (let [[k _] x]
         (and (keyword? k)
              (field-x? k)))))

(defn split-ref [x]
  (let [regex #"(?<=[a-z])(?=[A-Z])"]
    (str/split (name x) regex 2)))

(defn ->value-x [[k v]]
  (let [[_ Type] (split-ref k)]
    {:value {(keyword Type) v}}))

(defn extension? [x]
  (and (map? x)
       (:url x)
       (-> x (dissoc :url) not-empty)))

(defn ->extension [x]
  [x]
  {(:url x) (dissoc x :url)})

(defn extensions? [x]
  (and (map? x)
       (:extension x)))

(defn ->extensions [x]
  {(:url x) (apply merge (dissoc x :extension :url) (:extension x))})

(defn walker [x]
  (cond

    (reference? x)
    (->reference x)

    (and (map-node? x) (value-x? x))
    (->value-x x)

    (extensions? x)
    (->extensions x)

    (extension? x)
    (->extension x)

    :else x))

(defn ->aidbox [fhir]
  (walk/postwalk walker fhir))
