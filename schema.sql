drop table project_log;
drop table file_handle_log;
drop table replicas;
drop table file_handles;
drop table files;
drop table projects;

create table projects
(
	id	                bigserial primary key,
	owner	            text,
	created_timestamp   timestamp with time zone     default now(),
	state	            text,
    retry_count         int,
    attributes          jsonb  default '{}'::jsonb
);

create table files
(
    namespace text,
    name text,
    time_added  timestamp with time zone    default now(),
    primary key(namespace, name)
);

create index files_spec on files ((namespace || ':' || name));
    
create table replicas
(
    namespace   text,
    name        text,
    rse         text,
    path        text,
    url         text,
    available   boolean     default false,
    preference  int         default 0,
    primary key (namespace, name, rse),
    foreign key (namespace, name) references files (namespace, name)    on delete cascade
);

create index replicas_specs on replicas ((namespace || ':' || name));

create table file_handles
(
    project_id  bigint references projects(id) on delete cascade,
    id          bigserial primary key,
    namespace   text,
    name        text,
    state       text,
    worker_id   text,
    attempts    int default 0,
    attributes  jsonb  default '{}'::jsonb,
    foreign key (namespace, name) references files (namespace, name)
);

create unique index file_handles_project_id_filespec on file_handles(project_id, namespace, name);
create index file_handles_project_id on file_handles(project_id);
create index file_handles_filespec on file_handles(namespace, name);

create table project_log
(
    project_id  bigint,
    t           timestamp with time zone     default now(),
    type        text,
    message     text,
    primary key (project_id, t)
);

create table file_handle_log
(
    project_id  bigint,
    namespace   text,
    name        text,
    t           timestamp with time zone     default now(),
    type        text,
    message     text,
    primary key (project_id, namespace, name, t)
);

