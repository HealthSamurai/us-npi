(ns usnpi.conv
  (:require [clojure.string :as str]))

(defn jo [m]
  (let [sql ["jsonb_build_object("]]

    (conj sql ")")))


(defmulti to-sql (fn [acc x]
                   (cond
                     (and (map? x) (:$type x)) (:$type x)
                     (map? x) :object
                     (keyword? x) :column
                     (string? x) :literal
                     (vector? x) :array)))

(defn reduce-separted [sep f acc coll]
  (loop [[x & xs] coll
         acc acc]
    (cond
      (nil? xs) (f acc x)
      :else (recur
             xs
             (conj (f acc x) sep)))))

(defmethod to-sql
  :column
  [acc c]
  (conj acc (name c)))

(defmethod to-sql
  :literal
  [acc c]
  (conj acc (str "'" (name c) "'")))

(defmethod to-sql
  :object
  [acc m]
  (let [acc (conj acc "jsonb_strip_nulls(jsonb_build_object(")
        ]
    (->
     (reduce-separted "," (fn [acc [k v]]
                            (let [acc (conj acc (str "'" (name k) "',"))]
                              (to-sql acc v)) 
                            ) acc m)
     (conj "))"))))

(defmethod to-sql
  :array
  [acc v]
  (let [acc  (conj acc "(SELECT array_agg(x) from unnest(ARRAY[")]
    (-> (reduce-separted "," (fn [acc x] (to-sql acc x)) acc v)
        (conj "]) x where x is not null and x::text <> '{}' and x::text <> '[]')"))))

(defmethod to-sql
  :map
  [acc {col :column m :map}]
  (let [acc (conj acc "( CASE")]
    (->
     (reduce (fn [acc [k v]]
               (conj acc (str " WHEN "
                              (name col) " = '" k "'"
                              " THEN "
                              "'" v "'")))
             acc m)
     (conj "END )"))))

(defmethod to-sql
  :when
  [acc {col :column exp :expression}]
  (-> acc
      (conj (str "( CASE WHEN (")  (name col) " IS NOT NULL ) THEN ")
      (to-sql exp)
      (conj " ELSE NULL END)")))

(defn postfix [x p]
  (keyword (str (str (name x) "_" (str p)))))

(def conv
  {:name [{:given [:provider_first_name]
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

   :identifier (->> (range 1 50)
                    (mapv (fn [i]
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
                    (into []))

   :qualification (->> (range 1 10)
                    (mapv (fn [i]
                            {:$type :when
                             :column (postfix :provider_license_number i)
                             :expression {:identifier {:system "http://fhir.us/us-license"
                                                       :value (postfix :provider_license_number i)}
                                          :state      (postfix :provider_license_number_state_code i)
                                          :code       {:coding [{:system "http://nucc.org/provider-taxonomy"
                                                                 :code (postfix :healthcare_provider_taxonomy_code i)
                                                                 ;; :text (postfix :healthcare_provider_taxonomy_text i)
                                                                 }]}}}))
                    (into []))})

(comment
  (spit "/Users/nicola/usnpi/build/npi/tojson.sql"
        (str
         " truncate practitioner_json; \n"
         "insert into practitioner_json (id,resource) \n select npi, \n"
         (str/join " " (to-sql [] conv))
         "\nfrom practitioner
where entity_type_code = '1'
limit 10000
")))

