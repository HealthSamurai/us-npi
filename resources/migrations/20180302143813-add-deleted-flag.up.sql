begin;

alter table practitioner
    add column if not exists deleted boolean not null default false;

commit;
