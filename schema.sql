drop table project_log;
drop table file_handle_log;
drop table replicas cascade;
drop table file_handles;
drop table project_users
drop table projects;
drop table proximity_map;
drop table replica_log;
drop table rses;

create table if not exists users            -- do not create if we are adding DD schema to MetaCat
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text    default '',
    auth_info   jsonb   default '{}',
    auid        text                        -- anonymized user identificator
);

create table if not exists roles            -- do not create if we are adding DD schema to MetaCat
(
    name        text    primary key,
    parent_role text    references roles(name),
    description text
);

create table if not exists users_roles      -- do not create if we are adding DD schema to MetaCat
(
    username    text    references users(username),
    role_name        text    references roles(name),
    primary key(username, role_name)
);

create table projects
(
    id                  bigserial primary key,
    owner               text references users(username),
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
    project_id  bigint references projects(id),
    username    text references users(username)
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
    preference      int     default 0
);

create table replicas
(
    namespace   text,
    name        text,
    rse         text        references rses(name) on delete cascade,
    path        text,
    url         text,
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

    
