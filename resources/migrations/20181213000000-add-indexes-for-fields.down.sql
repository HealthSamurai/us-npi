begin;

drop index if exists pract_city_trgm_idx;
drop index if exists pract_zip_trgm_idx;
drop index if exists pract_state_trgm_idx;
drop index if exists pract_given_trgm_idx;
drop index if exists pract_family_trgm_idx;
drop index if exists pract_prefix_trgm_idx;
drop index if exists pract_suffix_trgm_idx;

commit;
