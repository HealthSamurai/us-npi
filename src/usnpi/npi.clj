(ns usnpi.npi
  (:require [honeysql.core :as honey]
            [honeysql.format :as sqlf]
            [clojure.java.jdbc :as jdbc]
            [usnpi.mapper :as mapper]
            [environ.core :as env]
            [clojure.string :as str]))


(def db {:dbtype "postgresql"
         :connection-uri (or (env/env :database-url) "jdbc:postgresql://localhost:5678/usnpi?stringtype=unspecified&user=postgres&password=verysecret")})

(defn strip-nils [r]
  (->>
   r
   (reduce (fn [acc [k v]]
             (if v (assoc! acc k v) acc)) (transient {}))
   (persistent!)))


(def dicts
  {:entity_type_code {1 "Individual"
                      2 "Organization"}
   :gender {"M" "male" "F" "female"}})

(def npi-identifier
  {:system "http://hl7.org/fhir/sid/us-npi"
   :type {:text "Provider number"
          :coding [{:code "PRN" :display "Provider number"}]}})

(defn postfix [x p]
  (keyword (str (str (name x) "_" (str p)))))

(defn clear-empty [x]
  (cond
    (map? x) (let [res  (reduce (fn [acc [k v]]
                                  (if-let [vv (clear-empty v)]
                                    (assoc acc k vv)
                                    acc)) {} x)]
               (when-not (empty? res) res))

    (vector? x) (let [res (filterv identity (map clear-empty x))]
                  (when-not (empty? res) res))

    (string? x) (when-not (str/blank? x) x)
    (not (nil? x)) x))




(defn to-iso-time  [x]
  (and x(let [[m d y] (str/split x #"/")]
          (str y "-" m "-" d))))

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
       ;; ext
       {:fhir [:identifier i :state] :npi [(postfix :other_provider_identifier_state i)]}
       ;; ext
       {:fhir [:identifier i :assigner] :npi [(postfix :other_provider_identifier_issuer i)]}
       {:fhir [:identifier i :type :coding 0 :code] :npi [(postfix :other_provider_identifier_type_code i)]}])
    (range 1 50))))

(def organization-mapping
  (concat
   [{:fhir [:resourceType] :fhir_const "Organization"}
    {:fhir [:id]  :npi [:npi]}
    {:fhir [:meta :lastUpdated]  :npi [:last_update_date] :to_fhir to-iso-time}

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
    {:fhir [:meta :lastUpdated]  :npi [:last_update_date] :to_fhir to-iso-time}
    {:fhir [:is_sole_proprietor]  :npi [:is_sole_proprietor]}
    {:fhir [:provider_enumeration_date]  :npi [:provider_enumeration_date] :to_fhir to-iso-time}

    {:fhir [:name {:use "official"} :given []]  :npi [:provider_first_name]}
    {:fhir [:name {:use "official"} :middle []]  :npi [:provider_middle_name]}
    {:fhir [:name {:use "official"} :family]   :npi [:provider_last_name_legal_name]}
    {:fhir [:name {:use "official"} :suffix []] :npi [:provider_name_suffix_text]}
    {:fhir [:name {:use "official"} :prefix []] :npi [:provider_name_prefix_text]}
    {:fhir [:name {:use "official"} :prefix []] :npi [:provider_credential_text]}

    {:fhir [:name {:use "usual"} :given 0]  :npi [:provider_other_first_name]}
    {:fhir [:name {:use "usual"} :given 1]  :npi [:provider_other_middle_name]}
    {:fhir [:name {:use "usual"} :family 0] :npi [:provider_other_last_name]}
    {:fhir [:name {:use "usual"} :suffix 0] :npi [:provider_other_name_suffix_text]}
    {:fhir [:name {:use "usual"} :prefix 0] :npi [:provider_other_name_prefix_text]}
    {:fhir [:name {:use "usual"} :prefix 1] :npi [:provider_other_credential_text]}

    {:fhir [:gender] :npi [:provider_gender_code] :to_fhir #(get {"M" "male" "F" "female"} %)}]

   shared-mapping

   (mapcat
    (fn [i]
      [{:fhir [:role (dec i) :identifier {:system "us-license"} :value]
        :npi [(postfix :provider_license_number i)]}
       {:fhir [:role (dec i) :identifier {:system "us-license"} :extension :state]
        :npi [(postfix :provider_license_number_state_code i)]}
       {:fhir [:role (dec i) :specialty 0 :coding {:system "http://nucc.org/provider-taxonomy"} :code]
        :npi [(postfix :healthcare_provider_taxonomy_code i)]}
       {:fhir [:role (dec i) :specialty 0 :coding {:system "http://nucc.org/provider-taxonomy"} :display]
        :npi [(postfix :healthcare_provider_taxonomy_text i)]}
       {:fhir [:role (dec i) :primary_taxonomy]
        :npi [(postfix :healthcare_provider_primary_taxonomy_switch i)]}])
    (range 1 10))))

(def pract-name-fields  (mapcat :npi (filter (fn [x] (= :name (first (:fhir x)))) practitioner-mapping)))
(def pract-state-fields (mapcat :npi (filter (fn [x]
                                               (and
                                                (= :address (first (:fhir x)))
                                                (= :state (last (:fhir x))))) practitioner-mapping)))

(def pract-mapped-fields
  (mapcat :npi practitioner-mapping))

(defn to-practitioner [o]
  (mapper/transform o practitioner-mapping [:npi :fhir]))



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


(defn migrate []
  (when-not (first (jdbc/query db "select c.relname from pg_index i, pg_class c  where c.oid = i.indexrelid and c.relname = 'trgm_idx'"))
    (jdbc/execute! db (index))
    (try
      (jdbc/query db "vacuum (analyze) practitioner")
      (catch Exception e (println "vacuum")))))

(comment
  (jdbc/execute! db "drop index trgm_idx")

  (index)
  (migrate)

  )


(defn elements [els rs]
  (if-not els
    rs
    (let [els (->> (str/split els #"\s*,\s*")
                   (map keyword))]
      (mapv (fn [x] (select-keys x els)) rs))))

(defn limit [x y]
  (str " limit " (or x y)))

(defn get-practitioners [{params :params :as req}]
  (if-not (:name params)
    {:status 422
     :body {:message "Please provide name="
            :example "?name=albert%20avila&_format=yaml&_elements=name,id&_count=100"}}
    (let [sql (str 
               "select * from practitioner where "
               (name-cond (:name params))
               (limit (:_count params) 100))]
      (println sql)
      {:status 200
       :body (->>
              (time (jdbc/query db sql))
              (mapv to-practitioner)
              (elements (:_elements params)))})))

(defn get-practitioner [{{npi :npi} :route-params :as req}]
  {:status 200
   :body (->>
          (time (jdbc/query db ["select * from practitioner where npi = ?" npi]))
          first
          to-practitioner)})

(defn get-practitioners-by-ids [{params :params :as req}]
  (if-let [ids  (:ids params)]
    (let [sql (format "select * from practitioner where npi in (%s)"
                  (->> (str/split ids #"\s*,\s*")
                      (mapv (fn [id] (str "'" (sanitize id) "'")))
                      (str/join ",")))]
      (println sql)
      {:status 200
       :body (->>
              (time (jdbc/query db sql))
              (mapv to-practitioner))})
    {:status 422
     :body {:message "ids parameter requried"}}))


