(ns usnpi.aidbox
  (:require [clojure.walk :as walk]
            [clojure.string :as str]))

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

;;
;; Reference
;;

(defn reference?
  [x]
  (:reference x))

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

(defn upd-reference [m]
  (if (reference? m)
    (->reference m)
    m))

;;

(defn upd-value-x [m]
  (if (:valueString m)
    (-> m
        (dissoc :valueString)
        (assoc-in [:value :String] (:valueString m)))
    m))

(defn upd-url [m]
  (if (:url m)
    {(:url m) (dissoc m :url)}
    m))

(defn upd-url-extension [m]
  (if (and (:url m) (:extension m))
    {(:url m) (apply merge (:extension m))}
    m))

(defn upd-extension [m]
  (if (and (:extension m) (not (:url m)))
    (apply merge (dissoc m :extension) (:extension m))
    m))

(defn bar [m]
  (if (map? m)
    (-> m
        upd-reference
        upd-value-x
        upd-extension
        upd-url-extension
        upd-url)
    m))

(defn ->aidbox4 [fhir]
  (walk/postwalk bar fhir))
