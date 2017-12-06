(ns usnpi.sync
  (:require [usnpi.util :as h]
            [clojure.string :as str]))

;; http://download.cms.gov/nppes/NPI_Files.html

(defn comma-join [xs] (str/join ",\n" xs))

(def text-pract-columns
  ["npi"
   "provider_organization_name_legal_business_name"
   "provider_last_name_legal_name"
   "provider_first_name"
   "provider_middle_name"
   "provider_credential_text"
   "provider_other_organization_name"
   "provider_other_last_name"
   "provider_other_first_name"
   "provider_other_middle_name"
   "provider_other_credential_text"
   "provider_first_line_business_mailing_address"
   "provider_second_line_business_mailing_address"
   "provider_business_mailing_address_city_name"
   "provider_business_mailing_address_state_name"
   "provider_first_line_business_practice_location_address"
   "provider_second_line_business_practice_location_address"
   "provider_business_practice_location_address_city_name"
   "provider_business_practice_location_address_state_name"])

(def indexed-text-pract-columns
  ["provider_license_number_"
   "healthcare_provider_taxonomy_text_"
   "other_provider_identifier_"])


(def text-org-columns
  ["npi"
   "provider_organization_name_legal_business_name"
   "provider_other_organization_name"
   "provider_first_line_business_mailing_address"
   "provider_second_line_business_mailing_address"
   "provider_business_mailing_address_city_name"
   "provider_business_mailing_address_state_name"
   "provider_business_mailing_address_postal_code"
   "provider_first_line_business_practice_location_address"
   "provider_second_line_business_practice_location_address"
   "provider_business_practice_location_address_city_name"
   "provider_business_practice_location_address_state_name"
   "provider_business_practice_location_address_postal_code"])


(def indexed-text-org-columns
  ["healthcare_provider_taxonomy_text_"
   "provider_license_number_"
   "other_provider_identifier_"
   "other_provider_identifier_state_"
   "healthcare_provider_taxonomy_group_"])

(defn additional-columns []
  (comma-join
   (for [i (range 1 16)]
     (h/str-template "ADD COLUMN healthcare_provider_taxonomy_text_~(i) text"))))

(defn set-additional-columns []
  (comma-join
   (for [i (range 1 16)]
     (h/str-template "healthcare_provider_taxonomy_text_~(i) = (SELECT TRIM(both ' ' from coalesce(nt.classification,'') || ' ' || coalesce(nt.specialization, '')) FROM nucc_taxonomy nt where nt.code = healthcare_provider_taxonomy_code_~(i) LIMIT 1)"))))

(defn search-pract-expr-sql []
  (str/join " || ' ' || \n"
            (concat
             (for [col text-pract-columns] (h/str-template "coalesce(~(col), '')"))
             (mapcat
              (fn [c] (for [i (range 1 10)] (h/str-template "coalesce(~(c)~(i), '')")))
              indexed-text-pract-columns))))

(defn search-org-expr-sql []
  (str/join " || ' ' || \n"
            (concat
             (for [col text-org-columns] (h/str-template "coalesce(~(col), '')"))
             (mapcat
              (fn [c] (for [i (range 1 10)] (h/str-template "coalesce(~(c)~(i), '')")))
              indexed-text-org-columns))))

(def monthes
  ["January"
   "February"
   "March"
   "April"
   "May"
   "June"
   "July"
   "August"
   "September"
   "October"
   "November"
   "December"])

(defn current-month []
  (nth monthes (.getMonth (java.util.Date.))))

(defn current-year []
  (+ 1900 (.getYear (java.util.Date.))))

(defn nucc-sql []
  (let [nucc-table-def (h/table-def-from-csv "nucc_taxonomy" "nucc-taxonomy.csv")
        nucc-csv (h/from-workdir "nucc-taxonomy.csv")]
    (h/str-template
     "-- nucc taxonomy
DROP TABLE IF EXISTS nucc_taxonomy;
~(nucc-table-def)
ALTER TABLE nucc_taxonomy ADD PRIMARY KEY (code);
COPY nucc_taxonomy FROM '~(nucc-csv)' with delimiter ',' CSV HEADER  encoding 'windows-1251';
CREATE INDEX nucc_taxonomy_code_idx ON nucc_taxonomy (code);
"
     )))

(defn csv-file-name []
  (-> (fn [f] (re-matches #"npidata_\d+-\d+.csv" (.getName f)))
      (filter (h/file-seq* ""))
      first
      .getName))

(defn npi-sql []
  (let [csv-file (csv-file-name)
        table-def (h/table-def-from-csv "practitioner" csv-file)
        npi-csv (str "/import/npi/data.csv")
        new-columns (additional-columns)
        update-columns (set-additional-columns)
        org-search-expr (search-org-expr-sql)]

(h/str-template
     "
DROP TABLE IF EXISTS practitioner;
~(table-def)
COPY practitioner FROM '~(npi-csv)' CSV HEADER NULL '';
CREATE TABLE organization (LIKE practitioner);
ALTER TABLE practitioner ADD PRIMARY KEY (npi);
DROP TABLE IF EXISTS organization;
INSERT INTO organization SELECT * FROM practitioner WHERE entity_type_code = '2';
--VACUUM FULL organization;
--VACUUM FULL practitioner;
    ")))


(defn init []
  (let [mon (current-month)
        year (str (current-year))
        arch-name (str mon "-" year ".zip")]
    (h/in-dir
     "npi"
     (when (not (h/exists? arch-name))
       (h/curl
        (format "http://download.cms.gov/nppes/NPPES_Data_Dissemination_%s_%s.zip" mon year)
        arch-name)
       (h/unzip arch-name))

     (when-not (h/exists? "nucc-taxonomy.csv")
       (h/curl "http://nucc.org/images/stories/CSV/nucc_taxonomy_161.csv" "nucc-taxonomy.csv"))

     (h/exec-in-current-dir! "chmod a+rw *csv")

     (let [csv (csv-file-name)]
       (h/exec-in-current-dir!
        (str "cat " csv " sed  -e 's/,\"\"/,/g'  | sed -e 's/\"<UNAVAIL>\"//g' > data.csv")))

     (h/spit* "index.sql" (str #_(nucc-sql) "\n" (npi-sql))))))

(comment
  (init)

  (h/in-dir
   "npi"
   (h/spit* "index.sql" (str (nucc-sql) "\n" (npi-sql))))

  )
