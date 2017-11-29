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
        table-def (h/table-def-from-csv "npi" csv-file)
        npi-csv (h/from-workdir csv-file)

        new-columns (additional-columns)
        update-columns (set-additional-columns)
        org-search-expr (search-org-expr-sql)]
(h/str-template
     "
DROP TABLE IF EXISTS npi;
~(table-def)
ALTER TABLE npi ADD PRIMARY KEY (npi);
COPY npi FROM '~(npi-csv)' CSV HEADER NULL '';

DROP TABLE IF EXISTS practitioner;
CREATE TABLE practitioner (LIKE npi);
ALTER TABLE practitioner ADD PRIMARY KEY (npi);
INSERT INTO practitioner SELECT * FROM npi WHERE entity_type_code = '1';
ALTER TABLE practitioner ~(new-columns);

UPDATE practitioner SET ~(update-columns);

ALTER TABLE practitioner ADD COLUMN search tsvector;

UPDATE practitioner SET search = (
   setweight(to_tsvector(coalesce(npi,'')),'A')
|| setweight(to_tsvector(coalesce(provider_organization_name_legal_business_name, '')), 'A')
|| setweight(to_tsvector(coalesce(provider_last_name_legal_name, '')), 'A')
|| setweight(to_tsvector(coalesce(provider_first_name, '')), 'B')
|| setweight(to_tsvector(coalesce(provider_middle_name, '')), 'B')
|| setweight(to_tsvector(coalesce(provider_credential_text, '')), 'B')
|| setweight(to_tsvector(coalesce(provider_other_organization_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_other_last_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_other_first_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_other_middle_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_other_credential_text, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_first_line_business_mailing_address, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_second_line_business_mailing_address, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_business_mailing_address_city_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_business_mailing_address_state_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_first_line_business_practice_location_address, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_second_line_business_practice_location_address, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_business_practice_location_address_city_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_business_practice_location_address_state_name, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_1, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_2, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_3, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_4, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_5, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_6, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_7, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_8, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_9, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_1, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_2, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_3, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_4, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_5, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_6, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_7, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_8, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_9, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_1, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_2, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_3, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_4, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_5, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_6, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_7, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_8, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_9, '')), 'D')
)
;

CREATE INDEX practitioner_ts_idx ON practitioner USING GIN (search);
DROP TABLE IF EXISTS organization;

CREATE TABLE organization (LIKE npi);
INSERT INTO organization SELECT * FROM npi WHERE entity_type_code = '2';
ALTER TABLE organization ADD PRIMARY KEY (npi);
ALTER TABLE organization ~(new-columns);
UPDATE organization SET ~(update-columns);

ALTER TABLE organization ADD COLUMN search tsvector;
UPDATE organization SET search = to_tsvector(' ' || ~(org-search-expr));

UPDATE organization SET search = (
setweight(to_tsvector(coalesce(npi, '')),'A')
|| setweight(to_tsvector(coalesce(provider_organization_name_legal_business_name, '')),'A')
|| setweight(to_tsvector(coalesce(provider_other_organization_name, '')),'A')
|| setweight(to_tsvector (coalesce(provider_first_line_business_mailing_address, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_second_line_business_mailing_address, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_business_mailing_address_city_name, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_business_mailing_address_state_name, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_business_mailing_address_postal_code, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_first_line_business_practice_location_address, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_second_line_business_practice_location_address, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_business_practice_location_address_city_name, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_business_practice_location_address_state_name, '')) , 'B')
|| setweight(to_tsvector(coalesce(provider_business_practice_location_address_postal_code, '')) , 'B')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_1, '')),'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_2, '')),'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_3, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_4, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_5, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_6, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_7, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_8, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_text_9, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_1, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_2, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_3, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_4, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_5, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_6, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_7, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_8, '')), 'D')
|| setweight(to_tsvector(coalesce(provider_license_number_9, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_1, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_2, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_3, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_4, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_5, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_6, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_7, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_8, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_9, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_1, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_2, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_3, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_4, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_5, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_6, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_7, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_8, '')), 'D')
|| setweight(to_tsvector(coalesce(other_provider_identifier_state_9, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_1, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_2, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_3, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_4, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_5, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_6, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_7, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_8, '')), 'D')
|| setweight(to_tsvector(coalesce(healthcare_provider_taxonomy_group_9, '')), 'D')
)
     ;
CREATE INDEX organization_ts_idx ON organization USING GIN (search);

VACUUM FULL organization;
VACUUM FULL practitioner;
DROP table npi;

    ")))


(defn init []
  (let [mon (current-month)
        arch-name (str mon ".zip")]
    (h/in-dir
     "npi"
     (when (not (h/exists? arch-name))
       (h/curl "http://download.cms.gov/nppes/NPPES_Data_Dissemination_October_2016.zip" arch-name)
       (h/unzip arch-name))

     (when-not (h/exists? "nucc-taxonomy.csv")
       (h/curl "http://nucc.org/images/stories/CSV/nucc_taxonomy_161.csv" "nucc-taxonomy.csv"))

     (h/exec-in-current-dir! "chmod a+rw *csv")

     (h/spit* "index.sql" (str (nucc-sql) "\n" (npi-sql))))))

(comment (init))
