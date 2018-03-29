(ns usnpi.aidbox
  (:require [clojure.walk :as walk]
            [clojure.string :as str]))

;;
;; X-fields
;;

;; https://www.hl7.org/fhir/extensibility.html#Extension
(def x-fields
  #{:valueInteger
    :valueUnsignedInt
    :valuePositiveInt
    :valueDecimal
    :valueDateTime
    :valueDate
    :valueTime
    :valueInstant
    :valueString
    :valueUri
    :valueOid
    :valueUuid
    :valueId
    :valueBoolean
    :valueCode
    :valueMarkdown
    :valueBase64Binary
    :valueCoding
    :valueCodeableConcept
    :valueAttachment
    :valueIdentifier
    :valueQuantity
    :valueSampledData
    :valueRange
    :valuePeriod
    :valueRatio
    :valueHumanName
    :valueAddress
    :valueContactPoint
    :valueTiming
    :valueReference
    :valueAnnotation
    :valueSignature
    :valueMeta})

(defn get-x-field [m]
  (some x-fields (keys m)))

(defn split-x-field [x]
  (let [regex #"(?<=[a-z])(?=[A-Z])"]
    (str/split (name x) regex 2)))

(defn ->value-x [k v]
  (let [[_ Type] (split-x-field k)]
    {:value {(keyword Type) v}}))

(defn get-x-value [m]
  (some-> m :value first second))

(defn upd-value-x [m]
  (if-let [x-field (get-x-field m)]
    (let [x-value (get m x-field)]
      (-> m
          (dissoc x-field)
          (merge (->value-x x-field x-value))))
    m))

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
  [{:keys [reference] :as m}]
  (cond

    (reference-id? m)
    (let [[resource id] (str/split reference #"/")]
      (-> m
          (dissoc :reference)
          (assoc :id id)
          (assoc :resourceType resource)))

    (reference-uri? m)
    (-> m
        (dissoc :reference)
        (assoc :uri reference))

    :else m))

(defn upd-reference [m]
  (if (reference? m)
    (->reference m)
    m))

;;
;; Extensions
;;

(defn url-shrink [url]
  (last (str/split url #"/")))

(defn upd-url [m]
  (if (:url m)
    (let [key (-> m :url url-shrink)]
      (if-let [x-value (get-x-value m)]
        {key x-value}
        {key (dissoc m :url)}))
    m))

(defn upd-url-extension [m]
  (if (and (:url m) (:extension m))
    (let [key (-> m :url url-shrink)]
      {key (apply merge (:extension m))})
    m))

(defn upd-extension [m]
  (if (and (:extension m) (not (:url m)))
    (apply merge (dissoc m :extension) (:extension m))
    m))

;;
;; Common
;;

(defn walker [m]
  (if (map? m)
    (-> m ;; the order matters
        upd-reference
        upd-value-x
        upd-extension
        upd-url-extension
        upd-url)
    m))

(defn ->aidbox [fhir]
  (walk/postwalk walker fhir))
