begin;

drop index if exists org_city_trgm_idx;
drop index if exists org_zip_trgm_idx;
drop index if exists org_state_trgm_idx;
drop index if exists org_name_trgm_idx;

commit;
