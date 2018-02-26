

(def shared-mapping
  (concat
   [{:fhir [:address {:use "work"} :city]       :npi [:provider_business_practice_location_address_city_name]}
    {:fhir [:address {:use "work"} :country]    :npi [:provider_business_practice_location_address_country_code_if_out]}
    {:fhir [:address {:use "work"} :line 0]     :npi [:provider_first_line_business_practice_location_address]}
    {:fhir [:address {:use "work"} :line 1]     :npi [:provider_second_line_business_practice_location_address]}
    {:fhir [:address {:use "work"} :state]      :npi [:provider_business_practice_location_address_state_name]}
    {:fhir [:address {:use "work"} :postalCode] :npi [:provider_business_practice_location_address_postal_code]}

    {:fhir [:address {:use "postal"} :city]       :npi [:provider_business_mailing_address_city_name]}
    {:fhir [:address {:use "postal"} :country]    :npi [:provider_business_mailing_address_country_code_if_outside_us]}
    {:fhir [:address {:use "postal"} :line 0]     :npi [:provider_first_line_business_mailing_address]}
    {:fhir [:address {:use "postal"} :line 1]     :npi [:provider_second_line_business_mailing_address]}
    {:fhir [:address {:use "postal"} :state]      :npi [:provider_business_mailing_address_state_name]}
    {:fhir [:address {:use "postal"} :postalCode] :npi [:provider_business_mailing_address_postal_code]}

    {:fhir [:telecom {:system "phone" :use "work"} :value] :npi [:provider_business_practice_location_address_telephone_number]}
    {:fhir [:telecom {:system "phone" :use "mailing"} :value] :npi [:provider_business_mailing_address_telephone_number]}
    {:fhir [:telecom {:system "fax" :use "work"} :value] :npi [:provider_business_practice_location_address_fax_number]}
    {:fhir [:telecom {:system "fax" :use "mailing"} :value] :npi [:provider_business_mailing_address_fax_number]}

    {:fhir [:identifier npi-identifier :value] :npi [:npi]}]

   (mapcat
    (fn [i]
      [{:fhir [:identifier i :value] :npi [(postfix :other_provider_identifier i)]}

       ;; to system
       {:fhir [:identifier i :state] :npi [(postfix :other_provider_identifier_state i)]}
       {:fhir [:identifier i :code] :npi [(postfix :other_provider_identifier_type_code i)]}
       {:fhir [:identifier i :issuer]  :npi [(postfix :other_provider_identifier_issuer i)]}


       ])
    (range 1 50))))

(def organization-mapping
  (concat
   [{:fhir [:resourceType] :fhir_const "Organization"}
    {:fhir [:id]  :npi [:npi]}
    {:fhir [:npi :lastUpdated]  :npi [:last_update_date] :to_fhir to-iso-time}

    {:fhir [:name]  :npi [:provider_organization_name_legal_business_name]}
    ;; other name
    {:fhir [:other_name] :npi [:provider_other_organization_name]}

    {:fhir [:type {:coding [{:system "http://nucc.org"}]} :coding 0 :value] :npi [:healthcare_provider_taxonomy_code]}

    {:fhir [:contact {:purpose {:text "ADMIN"}} :name :family 0]    :npi [:authorized_official_last_name]}
    {:fhir [:contact {:purpose {:text "ADMIN"}} :purpose :coding {:system "npi.org"} :code]    :npi [:authorized_official_title_or_position]}
    {:fhir [:contact {:purpose {:text "ADMIN"}} :name :given 0]    :npi [:authorized_official_first_name]}
    {:fhir [:contact {:purpose {:text "ADMIN"}} :name :given 1]    :npi [:authorized_official_middle_name]}
    {:fhir [:contact {:purpose {:text "ADMIN"}} :name :prefix 0]    :npi [:authorized_official_name_prefix_text]}
    {:fhir [:contact {:purpose {:text "ADMIN"}} :name :prefix 0]    :npi [:authorized_official_name_prefix_text]}

    {:fhir [:contact {:purpose {:text "ADMIN"}} :telecom {:system "phone"} :value]    :npi [:authorized_official_telephone_number]}]

   (mapcat
    (fn [i]
      [{:fhir [:type :coding i :code] :npi [(postfix :healthcare_provider_taxonomy_code (inc i))]}
       {:fhir [:type :coding i :system]
        :calculate [:type :coding i :code]
        :to_fhir (constantly "http://nucc.org/provider-taxonomy")}
       {:fhir [:type :coding i :display] :npi [(postfix :healthcare_provider_taxonomy_text (inc i))]}])
    (range 0 10))

   shared-mapping))

(def org-mapped-fields
  (mapcat :npi organization-mapping))

(defn to-organization [o]
  (let [ext (apply dissoc o org-mapped-fields)
        fhir (mapper/transform o organization-mapping [:npi :fhir])]
    (assoc-in fhir [:extension] (merge (or (:extension fhir) {}) ext))))

(defn mk-bundle [params resources]
  {:resourceType "Bundle"
   :type "searchset"
   :entry (map (fn [r] {:resource r}) resources)})

(def practitioner-mapping
  (concat
   [{:fhir [:resourceType] :fhir_const "Practitioner"}
    {:fhir [:id]  :npi [:npi]}
    ;; {:fhir [:npi :lastUpdated]  :npi [:last_update_date] :to_fhir to-iso-time}
    ;; {:fhir [:npi :is_sole_proprietor]  :npi [:is_sole_proprietor]}
    ;; {:fhir [:npi :provider_enumeration_date]  :npi [:provider_enumeration_date] :to_fhir to-iso-time}

    {:fhir [:name {:use "official"} :given []]  :npi [:provider_first_name]}
    {:fhir [:name {:use "official"} :given []]  :npi [:provider_middle_name]}
    {:fhir [:name {:use "official"} :family]   :npi [:provider_last_name_legal_name]}
    {:fhir [:name {:use "official"} :suffix []] :npi [:provider_name_suffix_text]}
    {:fhir [:name {:use "official"} :prefix []] :npi [:provider_name_prefix_text]}
    {:fhir [:name {:use "official"} :prefix []] :npi [:provider_credential_text]}

    {:fhir [:name {:use "usual"} :given []]  :npi [:provider_other_first_name]}
    {:fhir [:name {:use "usual"} :given []]  :npi [:provider_other_middle_name]}
    {:fhir [:name {:use "usual"} :family] :npi [:provider_other_last_name]}
    {:fhir [:name {:use "usual"} :suffix []] :npi [:provider_other_name_suffix_text]}
    {:fhir [:name {:use "usual"} :prefix []] :npi [:provider_other_name_prefix_text]}
    {:fhir [:name {:use "usual"} :prefix []] :npi [:provider_other_credential_text]}

    {:fhir [:gender] :npi [:provider_gender_code] :to_fhir #(get {"M" "male" "F" "female"} %)}]

   shared-mapping

   (mapcat
    (fn [i]

      [{:fhir [:qualification (dec i) :identifier {:system "http://fhir.us/us-license"} :value]
        :npi [(postfix :provider_license_number i)]}

       {:fhir [:qualification (dec i) :state]
        :npi [(postfix :provider_license_number_state_code i)]}

       {:fhir [:qualification (dec i) :code :coding {:system "http://nucc.org/provider-taxonomy"} :code]
        :npi [(postfix :healthcare_provider_taxonomy_code i)]}

       {:fhir [:qualification (dec i) :code :text]
        :npi [(postfix :healthcare_provider_taxonomy_text i)]}

       #_(postfix :healthcare_provider_taxonomy_text i)
       #_{:fhir [:qualification (dec i) :primary_taxonomy]
        :npi [(postfix :healthcare_provider_primary_taxonomy_switch i)]}])
    (range 1 10))))

(def pract-name-fields  (mapcat :npi (filter (fn [x] (= :name (first (:fhir x)))) practitioner-mapping)))
(def pract-state-fields (mapcat :npi (filter (fn [x]
                                               (and
                                                (= :address (first (:fhir x)))
                                                (= :state (last (:fhir x))))) practitioner-mapping)))

(def pract-mapped-fields
  (mapcat :npi practitioner-mapping))


(def identifier-codes
  {"01" "OTHER"
   "02" "MEDICARE UPIN"
   "04" "MEDICARE ID-TYPE UNSPECIFIED"
   "05" "MEDICAID"
   "06" "MEDICARE OSCAR/CERTIFICATION"
   "07" "MEDICARE NSC"
   "08" "MEDICARE PIN"})

(defn to-practitioner [o]
  (let [pr (mapper/transform o practitioner-mapping [:npi :fhir])]
    ;; FIX identifiers
    (update pr
            :identifier
            (fn [is]
              (->> is
                   (mapv (fn [x]
                           (if (or (:state x) (:issuer x) (:code x))
                             {:value (:value x)
                              :system (str "http://npiregistry.cms.gov/other-identifier?issuer=" (url-encode (or (:issuer x) (get identifier-codes (:code x)))) "&state=" (:state x) "&code=" (:code x))}
                             x))))))))



;; (jdbc/query db "select 1")
;; (jdbc/query db "select count(*) from practitioner")

;; (jdbc/query db "select count(*) from organization")


(def name-fields ["provider_first_name"
                 "provider_middle_name"
                 "provider_last_name_legal_name"
                 "provider_name_suffix_text"
                 "provider_name_prefix_text"
                 "provider_credential_text"
                 "provider_other_first_name"
                 "provider_other_middle_name"
                 "provider_other_last_name"
                 "provider_other_name_suffix_text"
                 "provider_other_name_prefix_text"
                 "provider_other_credential_text"])

(def names-expr
  (->> name-fields
       (mapv (fn [x] (str " lower(coalesce(" x ", ''))")))
       (str/join " || '$^' || ")
       (str "'^' || ")))

(defn sanitize [x]
  (str/replace x #"[^a-zA-Z0-9]" ""))

(defn name-cond [nm]
  (->> 
   (str/split (str/trim nm) #"\s+")
   (map (fn [x] (str "(" names-expr ") like  '%^" (sanitize x) "%'")))
   (str/join " AND ")))

#_(defn name-cond [nm]
  (str "(" names-expr ") ~  '("
       (->>
        (str/split (str/trim nm) #"\s+")
        (map sanitize)
        (map (fn [x] x))
        (str/join "|"))

       ")'"))



(name-cond "david")

(defn index []
  (str
   "
create extension if not exists pg_trgm;
CREATE INDEX trgm_idx ON practitioner USING GIST (
( " names-expr "  )
 gist_trgm_ops )"
   ))

