(ns usnpi.aidbox
  (:require [clojure.walk :as walk]
            [clojure.string :as str]

            ))

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

(defn value-x? [[k _]]
  (and (keyword? k)
       (re-matches #"^value[A-Z].*" (name k))))

(defn ->value-x [[k v]]
  (let [regex #"(?<=[a-z])(?=[A-Z])"
        [_ Type] (str/split (name k) regex 2)]
    {:value {(keyword Type) v}}))

(defn walker [x]
  (cond

    (reference? x)
    (->reference x)

    (and (map-node? x)
         (value-x? x))
    (->value-x x)

    :else x))

(defn ->aidbox [fhir]
  (walk/postwalk walker fhir))
