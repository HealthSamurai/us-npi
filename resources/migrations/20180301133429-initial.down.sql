begin;

drop index if exists pract_trgm_idx;

drop table if exists practitioner;

drop extension if exists pg_trgm;

commit;
