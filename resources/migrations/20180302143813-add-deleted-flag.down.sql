begin;

alter table practitioner
    drop column if exists deleted;

commit;
