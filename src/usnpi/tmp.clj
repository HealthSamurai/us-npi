(ns usnpi.tmp)

"




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




"
