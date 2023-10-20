drop table if exists project_log;
drop table if exists file_handle_log;
drop table if exists replicas cascade;
drop table if exists file_handles;
drop table if exists project_users;
drop table if exists project_roles;
drop table if exists projects;
drop table if exists proximity_map;
drop table if exists replica_log;
drop table if exists rses;

create table projects
(
    id                  bigserial primary key,
    owner               text references metacat.users(username),
    created_timestamp   timestamp with time zone     default now(),
    end_timestamp       timestamp with time zone,
    state	            text,
    retry_count         int,
    worker_timeout      interval,
    idle_timeout        interval,
    attributes          jsonb  default '{}'::jsonb,
    query               text
);

create table project_users
(
    project_id  bigint references projects(id) on delete cascade,
    username    text references metacat.users(username) on delete cascade,
    primary key(project_id, username)
);

create table project_roles
(
    project_id  bigint references projects(id) on delete cascade,
    role_name   text references metacat.roles(name) on delete cascade,
    primary key(project_id, role_name)
);

create table rses
(
    name            text    primary key,
    description     text    default '',
    is_enabled      boolean default false,
    is_available    boolean default false,
    is_tape         boolean default false,
    pin_url         text,
    poll_url        text,
    remove_prefix   text    default '',
    add_prefix      text    default '',
    pin_prefix      text    default '',
    preference      int     default 0,
    type            text
);

create table replicas
(
    namespace   text,
    name        text,
    rse         text        references rses(name) on delete cascade,
    path        text,
    url         text,
    urls        jsonb       default '[]'::jsonb,
    available   boolean     default false,
    preference  int         default 0,
    primary key (namespace, name, rse),
    foreign key (rse) references rses (name) on delete cascade
);

create index replicas_dids on replicas ((namespace || ':' || name));
create index replicas_rse on replicas(rse);

create view replicas_with_rse_availability as
    select replicas.*, rses.is_available as rse_available
        from replicas, rses
        where rses.name = replicas.rse and rses.is_enabled
;

create table file_handles
(
    project_id  bigint references projects(id) on delete cascade,
    namespace   text,
    name        text,
    state       text,
    worker_id   text,
    reserved_since  timestamp with time zone,
    attempts    int default 0,
    attributes  jsonb  default '{}'::jsonb,
    primary key (project_id, namespace, name)
);

create unique index file_handles_project_id_filespec on file_handles(project_id, namespace, name);
create index file_handles_project_id on file_handles(project_id);
create index file_handles_filespec on file_handles(namespace, name);

create table project_log
(
    project_id  bigint  references projects on delete cascade,
    t           timestamp with time zone     default now(),
    type        text,
    data        jsonb,
    primary key (project_id, t, type)
);

create table file_handle_log
(
    project_id  bigint,
    namespace   text,
    name        text,
    t           timestamp with time zone     default now(),
    type        text,
    data        jsonb,
    primary key (project_id, namespace, name, t, type),
    foreign key (project_id, namespace, name) references file_handles on delete cascade
);

create table replica_log
(
    namespace   text,
    name        text,
    rse         text,
    t           timestamp with time zone     default now(),
    type        text,
    data        jsonb,
    primary key (namespace, name, rse, t, type),
    foreign key (namespace, name, rse) references replicas(namespace, name, rse) on delete cascade,
    foreign key (rse) references rses(name) on delete cascade
);

create table proximity_map
(
    cpu             text,
    rse             text,
    proximity       int,
    primary key (cpu, rse)
);

insert into proximity_map(cpu, rse, proximity) values('DEFAULT', 'DEFAULT', 100);

    
