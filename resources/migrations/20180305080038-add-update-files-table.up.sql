begin;

create table if not exists npi_updates (
    id serial primary key,
    type text not null,
    url text not null,
    date timestamp with timezone not null default current_timestamp at time zone 'UTC',
    success boolean no null default false,
    message text not null default ''
);

commit;
