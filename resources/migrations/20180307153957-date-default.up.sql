begin;

alter table npi_updates
    alter column date set default current_timestamp;

commit;
