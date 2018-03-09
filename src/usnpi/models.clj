(ns usnpi.models
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]))


;;
;; spec and tools
;;

(defn- postfix [field i]
  (keyword (format "%s_%s" (name field) i)))

(defn- spec-dispatch [spec _]
  (cond
    (map? spec)
    (if-let [map-type (:$type spec)]
      map-type :object)
    (vector? spec) :vector
    (keyword? spec) :keyword))

(defmulti ^:private
  ->practitioner spec-dispatch)

(defmethod ->practitioner :default
  [spec _] spec)

(defmethod ->practitioner :object
  [spec-map data]
  (not-empty
   (into {} (for [[k v] spec-map
                  :let [v-new (->practitioner v data)]
                  :when v-new]
              [k v-new]))))

(defmethod ->practitioner :keyword
  [spec-kw data]
  (get data spec-kw))

(defmethod ->practitioner :vector
  [spec-vec data]
  (not-empty
   (remove nil? (for [spec-item spec-vec]
                  (->practitioner spec-item data)))))

(defmethod ->practitioner :when
  [{:keys [column expression]} data]
  (when-let [field (->practitioner column data)]
    (->practitioner expression data)))

(defmethod ->practitioner :map
  [{:keys [column map]} data]
  (when-let [field (->practitioner column data)]
    (get map field)))

(def ^:private
  spec-practitioner
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

(defn- prepare-header-field [field]
  (-> field
      str/trim
      str/lower-case
      (str/replace #"[^a-z0-9 _]" "")
      (str/replace #"\s+" "_")
      keyword))

(defn- prepare-row-field [field]
  (when-not (or (= field "") (= field "<UNAVAIL>"))
    field))

(defn- read-csv
  [src]
  (let [reader (io/reader src)
        rows (csv/read-csv reader)
        header (map prepare-header-field (first rows))]
    (for [row (rest rows)]
      (zipmap header (map prepare-row-field row)))))

;;
;; Models
;;

(defn read-practitioners
  [src]
  (for [data (read-csv src)]
    (->practitioner spec-practitioner data)))
