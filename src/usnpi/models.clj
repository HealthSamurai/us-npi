(ns usnpi.models
  "Tools and functions to process business models."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

;;
;; Rules and tools
;;

(defn- postfix
  "Adds an index to the end of a keyword."
  [field i]
  (keyword (format "%s_%s" (name field) i)))

(defn- rule-dispatch
  "Checks how to treat a rule. Maps with a `:$type` field
  are processed in a special way."
  [rule _]
  (cond
    (map? rule)
    (if-let [map-type (:$type rule)]
      map-type :object)
    (vector? rule) :array
    (keyword? rule) :keyword))

(defmulti
  ^{:private true
    :doc "Takes a rule map and a data map and returns a new model map."
    :arglists '([rule data])}
  rule-expand rule-dispatch)

(defmethod rule-expand :default
  [rule _] rule)

(defmethod rule-expand :object
  [rule-map data]
  (not-empty
   (into {} (for [[k v] rule-map
                  :let [v-new (rule-expand v data)]
                  :when v-new]
              [k v-new]))))

(defmethod rule-expand :keyword
  [rule-kw data]
  (get data rule-kw))

(defmethod rule-expand :array
  [rule-vec data]
  (not-empty
   (remove nil? (for [rule-item rule-vec]
                  (rule-expand rule-item data)))))

(defmethod rule-expand :when
  [{:keys [column expression]} data]
  (when-let [field (rule-expand column data)]
    (rule-expand expression data)))

(defmethod rule-expand :map
  [{:keys [column map]} data]
  (when-let [field (rule-expand column data)]
    (get map field)))

(def ^:private
  rule-practitioner
  {:id :npi
   :resourceType "Practitioner"

   :name [{:given [:provider_first_name]
           :family :provider_last_name_legal_name
           :middle [:provider_middle_name]
           :suffix [:provider_name_suffix_text]
           :prefix [:provider_name_prefix_text
                    :provider_credential_text]}

          {:given  [:provider_other_first_name]
           :middle [:provider_other_middle_name]
           :family :provider_other_last_name
           :suffix [:provider_other_name_suffix_text]
           :prefix [:provider_other_name_prefix_text
                    :provider_other_credential_text]}]

   :gender {:$type :map
            :column :provider_gender_code,
            :map {"F" "female" "M" "male"}}

   :address [{:user "work"
              :city :provider_business_practice_location_address_city_name
              :country :provider_business_practice_location_address_country_code_if_out
              :line [:provider_first_line_business_practice_location_address
                     :provider_second_line_business_practice_location_address]
              :state :provider_business_practice_location_address_state_name
              :postalCode :provider_business_practice_location_address_postal_code}]

   :telecom [{:$type :when
              :column :provider_business_practice_location_address_telephone_number
              :expression {:system "phone"
                           :user "work"
                           :value :provider_business_practice_location_address_telephone_number}}

             {:$type :when
              :column :provider_business_mailing_address_telephone_number
              :expression {:system "phone"
                           :user "mailing"
                           :value :provider_business_mailing_address_telephone_number}}

             {:$type :when
              :column  :provider_business_practice_location_address_fax_number
              :expression {:system "fax"
                           :user "work"
                           :value :provider_business_practice_location_address_fax_number}}

             {:$type :when
              :column :provider_business_mailing_address_fax_number
              :expression {:system "fax"
                           :user "mailing"
                           :value :provider_business_mailing_address_fax_number}}]

   :identifier (vec
                (for [i (range 1 50)]
                  {:value  (postfix :other_provider_identifier i)
                   :state  (postfix :other_provider_identifier_state i)
                   :code   {:$type :map
                            :column (postfix :other_provider_identifier_type_code i)
                            :map {"01" "OTHER"
                                  "02" "MEDICARE UPIN"
                                  "04" "MEDICARE ID-TYPE UNSPECIFIED"
                                  "05" "MEDICAID"
                                  "06" "MEDICARE OSCAR/CERTIFICATION"
                                  "07" "MEDICARE NSC"
                                  "08" "MEDICARE PIN"}}
                   :issuer (postfix :other_provider_identifier_issuer i)}))

   :qualification (vec
                   (for [i (range 1 10)]
                     {:$type :when
                      :column (postfix :provider_license_number i)
                      :expression {:identifier {:system "http://fhir.us/us-license"
                                                :value (postfix :provider_license_number i)}
                                   :state      (postfix :provider_license_number_state_code i)
                                   :code       {:coding [{:system "http://nucc.org/provider-taxonomy"
                                                          :code (postfix :healthcare_provider_taxonomy_code i)}]}}}))})

;;
;; CSV reading
;;

(defn- prepare-header-field
  "Turns a CSV header field into a keyword."
  [field]
  (-> field
      str/trim
      str/lower-case
      (str/replace #"[^a-z0-9 _]" "")
      (str/replace #"\s+" "_")
      keyword))

(defn- prepare-row-field
  "Cleans dummy values from a CSV field."
  [field]
  (when-not (or (= field "") (= field "<UNAVAIL>"))
    field))

(defn- read-csv
  "Returns a lazy sequence of maps.
  The `src` is either a file path or an input stream."
  [src]
  (let [reader (io/reader src)
        rows (csv/read-csv reader)
        header (map prepare-header-field (first rows))]
    (for [row (rest rows)]
      (zipmap header (map prepare-row-field row)))))

;;
;; Models
;;

(def ->practitioner (partial rule-expand rule-practitioner))

(defn read-practitioners
  "Returns a lazy sequence of `Practitioner` maps.
  The `src` is either a file path or an input stream."
  [src]
  (for [data (read-csv src)
        :when (-> data :entity_type_code (= "1"))]
    (->practitioner data)))
