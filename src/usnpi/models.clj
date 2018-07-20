(ns usnpi.models
  "Tools and functions to process business models.
  http://download.cms.gov/nppes/NPI_Files.html
  http://www.hipaaspace.com/documentation/API/Medical%20Web%20Services/NPI%20Fields.html"
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [ring.util.codec :refer [form-encode]]))

;;
;; URLs
;;

(defn drop-nil
  "Drops nil values from a map."
  [m]
  (into {} (filter (fn [[k v]] (-> v nil? not)) m)))

(defn url-npi
  "Returns an NPI-relates URL to use in `:system` fields."
  [field & [params]]
  (let [qs (not-empty (-> params drop-nil form-encode))]
    (format "http://npiregistry.cms.hhs.gov/%s%s%s"
            field
            (if qs "?" "")
            (or qs ""))))

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

(defmethod rule-expand :url-npi
  [{:keys [field params]} data]
  (url-npi (rule-expand field data) (rule-expand params data)))

(defmethod rule-expand :map
  [{:keys [column map]} data]
  (when-let [field (rule-expand column data)]
    (get map field)))

(defmethod rule-expand :join
  [{:keys [values separator]} data]
  (let [coll (rule-expand values data)]
    (not-empty
     (str/trim
      (str/join separator coll)))))

(def ^{:private true
       :doc "Common address part."}
  rule-address
  [{:use "work"
    :city :provider_business_practice_location_address_city_name
    :country :provider_business_practice_location_address_country_code_if_out
    :line [:provider_first_line_business_practice_location_address
           :provider_second_line_business_practice_location_address]
    :state :provider_business_practice_location_address_state_name
    :postalCode :provider_business_practice_location_address_postal_code}])

(def ^{:private true
       :doc "Common telecom part."}
  rule-telecom
  [{:$type :when
    :column :provider_business_practice_location_address_telephone_number
    :expression {:system "phone"
                 :use "work"
                 :value :provider_business_practice_location_address_telephone_number}}

   {:$type :when
    :column :provider_business_mailing_address_telephone_number
    :expression {:system "phone"
                 :use "mailing"
                 :value :provider_business_mailing_address_telephone_number}}

   {:$type :when
    :column  :provider_business_practice_location_address_fax_number
    :expression {:system "fax"
                 :use "work"
                 :value :provider_business_practice_location_address_fax_number}}

   {:$type :when
    :column :provider_business_mailing_address_fax_number
    :expression {:system "fax"
                 :use "mailing"
                 :value :provider_business_mailing_address_fax_number}}])

(def ^{:private true
       :doc "https://www.hl7.org/fhir/organization-examples.html"}
  rule-organization
  {:id :npi
   :resourceType "Organization"
   :name :provider_organization_name_legal_business_name
   :telecom rule-telecom
   :address rule-address

   :type [{:coding [{:system "http://nucc.org"
                     :code :healthcare_provider_taxonomy_code_1}]}]

   :contact
   [{:purpose
     {:coding [{:system "http://hl7.org/fhir/contactentity-type"
                :code :authorized_official_title_or_position}]}}

    :name {:text {:$type :join
                  :separator " "
                  :values [:authorized_official_first_name
                           :authorized_official_middle_name
                           :authorized_official_last_name]}}

    :telecom [{:$type :when
               :column :provider_business_practice_location_address_telephone_number
               :expression {:system "phone"
                            :use "work"
                            :value :provider_business_practice_location_address_telephone_number}}]]})

(def ^:private
  rule-practitioner
  {:id :npi
   :resourceType "Practitioner"

   ;; https://www.hl7.org/fhir/datatypes.html#HumanName
   :name [{:given [:provider_first_name :provider_middle_name]
           :family :provider_last_name_legal_name
           :suffix [:provider_name_suffix_text]
           :prefix [:provider_name_prefix_text
                    :provider_credential_text]}

          {:given  [:provider_other_first_name :provider_other_middle_name]
           :family :provider_other_last_name
           :suffix [:provider_other_name_suffix_text]
           :prefix [:provider_other_name_prefix_text
                    :provider_other_credential_text]}]

   :gender {:$type :map
            :column :provider_gender_code,
            :map {"F" "female" "M" "male"}}

   :address rule-address
   :telecom rule-telecom

   :identifier
   (vec
    (for [i (range 1 50)]
      {:$type :when
       :column (postfix :other_provider_identifier i)
       :expression {:system {:$type :url-npi
                             :field "identifier"
                             :params {:state (postfix :other_provider_identifier_state i)
                                      :issuer (postfix :other_provider_identifier_issuer i)
                                      :type_code (postfix :other_provider_identifier_type_code i)}}
                    :type {:$type :when
                           :column (postfix :other_provider_identifier_type_code i)
                           :expression {:coding [{:code {:$type :map
                                                         :column (postfix :other_provider_identifier_type_code i)
                                                         :map {"01" "OTHER"
                                                               "02" "MEDICARE UPIN"
                                                               "04" "MEDICARE ID-TYPE UNSPECIFIED"
                                                               "05" "MEDICAID"
                                                               "06" "MEDICARE OSCAR/CERTIFICATION"
                                                               "07" "MEDICARE NSC"
                                                               "08" "MEDICARE PIN"}}}]}}
                    :value (postfix :other_provider_identifier i)}}))

   :qualification
   (vec
    (for [i (range 1 15)]
      {:code {:coding [{:$type :when
                        :column (postfix :healthcare_provider_taxonomy_code i)
                        :expression {:system {:$type :url-npi
                                              :field "taxonomy_code"}
                                     :code (postfix :healthcare_provider_taxonomy_code i)}}]}

       :identifier [{:$type :when
                     :column (postfix :provider_license_number i)
                     :expression {:system {:$type :url-npi
                                           :field "license_number"
                                           :params {:state (postfix :provider_license_number_state_code i)}}
                                  :value (postfix :provider_license_number i)}}]}))})

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

(defmulti ->model
  "Turns a CSV map into a FHIR entity."
  {:arglists '([data])}
  :entity_type_code)

(defmethod ->model :default
  [data] nil)

(defmethod ->model "1"
  [data]
  (rule-expand rule-practitioner data))

(defmethod ->model "2"
  [data]
  (rule-expand rule-organization data))

(defn read-models
  "Returns a lazy sequence of FHIR models.
  The `src` is either a file path or an input stream."
  [src]
  (->> (read-csv src)
       (map ->model)
       (remove nil?)))

(defn- model?
  [resourceType model]
  (and (map? model) (= (:resourceType model) resourceType)))

(def practitioner? (partial model? "Practitioner"))

(def organization? (partial model? "Organization"))
