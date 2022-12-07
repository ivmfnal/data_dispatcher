import json, time, io, traceback, urllib.parse
from datetime import datetime, timedelta, timezone
from metacat.auth import BaseDBUser as DBUser

def cursor_iterator(c):
    t = c.fetchone()
    while t is not None:
        yield t
        t = c.fetchone()


def json_literal(v):
    if isinstance(v, str):       v = '"%s"' % (v,)
    elif isinstance(v, bool):    v = "true" if v else "false"
    elif v is None:              v = "null"
    else:   v = str(v)
    return v
    
class DBObject(object):
    
    @classmethod
    def from_tuple(cls, db, dbtup):
        h = cls(db, *dbtup)
        return h
    
    @classmethod
    def columns(cls, table_name=None, as_text=True, exclude=[]):
        if isinstance(exclude, str):
            exclude = [exclude]
        clist = [c for c in cls.Columns if c not in exclude]
        if table_name:
            clist = [table_name+"."+cn for cn in clist]
        if as_text:
            return ",".join(clist)
        else:
            return clist
    
    @classmethod
    def pk_columns(cls, table_name=None, as_text=True, exclude=[]):
        if isinstance(exclude, str):
            exclude = [exclude]
        clist = [c for c in cls.PK if c not in exclude]
        if table_name:
            clist = [table_name+"."+cn for cn in clist]
        if as_text:
            return ",".join(clist)
        else:
            return clist
    
    def pk(self):       # return PK values as a tuple in the same order as cls.PK
        raise NotImplementedError()
    
    @classmethod
    def get(cls, db, *pk_vals):
        pk_cols_values = [f"{c} = %s" for c in cls.PK]
        where = " and ".join(pk_cols_values)
        cols = ",".join(cls.Columns)
        c = db.cursor()
        c.execute(f"select {cols} from {cls.Table} where {where}", pk_vals)
        tup = c.fetchone()
        if tup is None: return None
        else:   return cls.from_tuple(db, tup)

    def _delete(self, cursor=None, do_commit=True, **pk_values):
        cursor = cursor or self.DB.cursor()
        where_clause = " and ".join(f"{column} = '{value}'" for column, value in pk_values.items())
        try:
            cursor.execute(f"""
                delete from {self.Table} where {where_clause}
            """)
            if do_commit:
                cursor.execute("commit")
        except:
            cursor.execute("rollback")
            raise
    
    @classmethod
    def list(cls, db):
        columns = cls.columns(as_text=True)
        table = cls.Table
        c = db.cursor()
        c.execute(f"select {columns} from {table}")
        return (cls.from_tuple(db, tup) for tup in cursor_iterator(c))
    
    def delete(self, cursor=None, do_commit=True):
        pk_values = {column:value for column, value in zip(self.PK, self.pk())}
        return self._delete(cursor=None, do_commit=True, **pk_values)
    
class DBManyToMany(object):
    
    def __init__(self, db, table, src_fk_values, dst_fk_columns, payload_columns, dst_class):
        self.DB = db
        self.Table = table
        self.SrcFKColumns, self.SrcFKValues = zip(*list(src_fk_values.items()))
        self.DstFKColumns = dst_fk_columns
        self.DstClass = dst_class
        self.DstTable = dst_class.Table
        self.DstPKColumns = self.DstTable.PK

    def add(self, dst_pk_values, payload, cursor=None, do_commit=True):
        assert len(dst_pk_values) == len(self.DstFKColumns)
        
        payload_cols_vals = list(payload.items())
        payload_cols, payload_vals = zip(*payload_cols_vals)
        
        fk_cols = ",".join(self.SrcFKColumns + self.DstFKColumns)
        cols = ",".join(self.SrcFKColumns + self.DstFKColumns + payload_cols)
        vals = ",".join([f"'{v}'" for v in self.SrcFKValues + dst_pk_values + payload_vals])
        
        if cursor is None: cursor = self.DB.cursor()
        try:
            cursor.execute(f"""
                insert into {self.Table}({cols}) values({vals})
                    on conflict({fk_cols}) do nothing
            """)
            if do_commit:
                cursor.execute("commit")
        except:
            cursor.execute(rollback)
            raise
        return self

    def list(self, cursor=None):
        out_columns = ",".join(f"{self.DstTable}.{c}" for c in self.DstClass.Columns)
        join_column_pairs = [
            (f"{self.Table}.{dst_fk}", f"{self.DstTable}.{dst_pk}") 
            for src_fk, dst_pk in zip(self.DstFKColumns, self.DstPKColumns)
        ]
        join_condition = " and ".join(f"{fk} = {pk}" for fk, pk in join_column_pairs)
        if cursor is None: cursor = self.DB.cursor()
        cursor.execute(f"""
            select {out_columns}
                from {self.DstTable}, {self.Table}
                where {join_condition}
        """)
        return (self.DstClass.from_tuple(self.DB, tup) for tup in fetch_generator(cursor))
        
    def __iter__(self):
        return self.list()

class DBOneToMany(object):
    
    def __init__(self, db, table, src_pk_values, dst_fk_columns, dst_class):
        self.DB = db
        self.Table = table
        self.SrcPKColumns, self.SrcPKValues = zip(*list(src_pk_values.items()))
        self.DstClass = dst_class
        self.DstTable = dst_class.Table
        self.DstFKColumns = dst_fk_columns

    def list(self, cursor=None):
        out_columns = ",".join(f"{self.DstTable}.{c}" for c in self.DstClass.Columns)
        join_column_pairs = [
            (f"{self.Table}.{dst_pk}", f"{self.DstTable}.{dst_fk}") 
            for src_pk, dst_fk in zip(self.SrcPKColumns, self.DstFKColumns)
        ]
        join_condition = " and ".join(f"{pk} = {fk}" for pk, fk in join_column_pairs)
        if cursor is None: cursor = self.DB.cursor()
        cursor.execute(f"""
            select {out_columns}
                from {self.DstTable}, {self.Table}
                where {join_condition}
        """)
        return (self.DstClass.from_tuple(self.DB, tup) for tup in fetch_generator(cursor))
        
    def __iter__(self):
        return self.list()

class DBLogRecord(object):

    def __init__(self, type, t, data, id_columns = {}):
        self.Type = type
        self.T = t
        self.Data = data
        self.IDColumns = id_columns             # {name->value}

    def __str__(self):
        return "DBLogRecord(%s, %s, <%s>, {%s})" % (self.Type, self.T, 
            ", ".join(["%s=%s" % kv for kv in sorted(self.IDColumns.items())]),
            ", ".join(["%s=%s" % kv for kv in sorted(self.Data.items())])
       )

    def __getattr__(self, key):
        return getattr(self.Data, key)

    def __getitem__(self, key):
        return self.Data[key]
        
    def as_jsonable(self):
        out = dict(
            type = self.Type,
            t = self.T.timestamp(),
            data = self.Data
        )
        if self.IDColumns:
            out.update(self.IDColumns)
        return out


class HasLogRecord(object):
    
    #
    # uses class attributes:
    #
    #   LogTable        - name of the table to store the log
    #   LogIDColumns    - list of columns in the log table identifying the parent
    #

    def add_log(self, type, data=None, **kwargs):
        #print("add_log:", type, data, kwargs)
        c = self.DB.cursor()
        data = (data or {}).copy()
        data.update(kwargs)
        parent_pk_columns = ",".join(self.LogIDColumns)
        parent_pk_values = ",".join([f"'{v}'" for v in self.pk()])
        c.execute(f"""
            begin;
            insert into {self.LogTable}({parent_pk_columns}, type, data)
                values({parent_pk_values}, %s, %s);
            commit
        """, (type, json.dumps(data)))
        
    @classmethod
    def log_records(cls, db, **selection):
        c = db.cursor()
        columns = ",".join(["type", "t", "data"] + cls.LogIDColumns)
        sort_columns = ",".join(cls.LogIDColumns + ["t"])
        wheres = ["true"] + [f"{name}='{value}'" for name, value in selection.items()]
        wheres = " and ".join(wheres)
        c.execute(f"""
            select {columns}
                from {cls.LogTable}
                where {wheres}
                order by {sort_columns}
        """)
        for tup in cursor_iterator(c):
            type, t, data = tup[:3]
            id_columns = { name:value for name, value in zip(cls.LogIDColumns, tup[3:]) }
            yield DBLogRecord(type, t, data, id_columns)

    @classmethod
    def add_log_bulk(cls, db, records):
        """
            records: list of tuples:
                (
                    (
                        id_column1_value,
                        id_column2_value, ...
                    ),
                    type,
                    { data }
                )
        """
        csv = []
        for id_values, type, data in records:
            row = '\t'.join([str(v) for v in id_values] + [type, json.dumps(data)])
            csv.append(row)
        if csv:
            csv = io.StringIO("\n".join(csv))

            table = cls.LogTable
            columns = cls.LogIDColumns + ["type", "data"]
            c = db.cursor()
            try:
                c.execute("begin")
                c.copy_from(csv, table, columns=columns)
                c.execute("commit")
            except:
                c.execute("rollback")
                raise

    def get_log(self, type=None, since=None, reversed=False):
        parent_pk_columns = self.LogIDColumns
        parent_pk_values = self.pk()
        wheres = [f"{c} = '{v}'" for c, v in zip(parent_pk_columns, parent_pk_values)]
        if isinstance(since, (float, int)):
            since = datetime.utcfromtimestamp(since).replace(tzinfo=timezone.utc)
            wheres.append(f"t >= {since}")
        if type is not None:
            wheres.append(f"type = '{type}'")
        wheres = " and ".join(wheres)
        desc = "desc" if reversed else ""
        sql = f"""
            select type, t, data from {self.LogTable}
                where {wheres}
                order by t {desc}
        """
        c = self.DB.cursor()
        c.execute(sql)
        return (DBLogRecord(type, t, message) for type, t, message in cursor_iterator(c))


class DBProject(DBObject, HasLogRecord):
    
    InitialState = "active"
    States = ["active", "failed", "done", "cancelled", "held"]
    EndStates = ["failed", "done", "cancelled"]
    
    Columns = "id,owner,created_timestamp,end_timestamp,state,retry_count,attributes,query,worker_timeout".split(",")
    Table = "projects"
    PK = ["id"]
    
    LogIDColumns = ["project_id"]
    LogTable = "project_log"
    
    def __init__(self, db, id, owner=None, created_timestamp=None, end_timestamp=None, state=None, 
                retry_count=None, attributes={}, query=None, worker_timeout=None):
        self.DB = db
        self.ID = id
        self.Owner = owner
        self.State = state
        self.CreatedTimestamp = created_timestamp               # datetime
        self.RetryCount = retry_count
        self.Attributes = (attributes or {}).copy()
        self.Handles = None
        self.HandleCounts = None
        self.EndTimestamp = end_timestamp
        self.Query = query
        self.WorkerTimeout = worker_timeout
        
    def pk(self):
        return (self.ID,)
        
    def quoted_query(self):
        query = self.Query
        if query:
            query = urllib.parse.quote_plus(query)
        return query
        
    def time_since_created(self, t=None):
        if self.CreatedTimestamp is None:
            return None
        if t is None: t = time.time()
        t_created = self.CreatedTimestamp.timestamp()
        if isinstance(t, datetime):
            t = t.timestamp()
        return t - t_created

    def as_jsonable(self, with_handles=False, with_replicas=False):
        #print("Project.as_jsonable: with_handles:", with_handles, "   with_replicas:", with_replicas)
        out = dict(
            project_id = self.ID,
            owner = self.Owner,
            state = self.State,
            retry_count = self.RetryCount,
            attributes = self.Attributes or {},
            created_timestamp = self.CreatedTimestamp.timestamp(),
            ended_timestamp = None if self.EndTimestamp is None else self.EndTimestamp.timestamp(),
            active = self.is_active(),
            query = self.Query,
            worker_timeout = self.WorkerTimeout
        )
        if with_handles or with_replicas:
            out["file_handles"] = [h.as_jsonable(with_replicas=with_replicas) for h in self.handles()]
            #print("Project.as_jsonable: handles:", out["file_handles"])
        return out

    def attributes_as_json(self):
        return json.dumps(self.Attributes, indent=4)
        
    @staticmethod
    def create(db, owner, retry_count=None, attributes={}, query=None, worker_timeout=None):
        if isinstance(owner, DBUser):
            owner = owner.Username
        c = db.cursor()
        try:
            c.execute("begin")
            c.execute("""
                insert into projects(owner, state, retry_count, attributes, query, worker_timeout)
                    values(%s, %s, %s, %s, %s, %s)
                    returning id
            """, (owner, DBProject.InitialState, retry_count, json.dumps(attributes or {}), query, worker_timeout))
            id = c.fetchone()[0]
            db.commit()
        except:
            db.rollback()
            raise
            
        project = DBProject.get(db, id)
        return project

    @staticmethod
    def list(db, owner=None, state=None, not_state=None, attributes=None, with_handle_counts=False):
        wheres = ["true"]
        if owner: wheres.append(f"p.owner='{owner}'")
        if state: wheres.append(f"p.state='{state}'")
        if not_state: wheres.append(f"p.state!='{not_state}'")
        if attributes is not None:
            for name, value in attributes.items():
                wheres.append("p.attributes @> '{\"%s\": %s}'::jsonb" % (name, json_literal(value)))
        p_wheres = " and ".join(wheres)
        c = db.cursor()
        table = DBProject.Table
        columns = DBProject.columns("p", as_text=True)
        if with_handle_counts:
            h_table = DBFileHandle.Table
            rse_table = DBRSE.Table
            rep_table = DBReplica.Table

            #
            # Get active replicas counts per project
            #
            
            available_by_project = {}
            saved = f"""
                with 
                    found_files as  
                    (
                        select distinct h.project_id, r.namespace, r.name, true as found
                            from {rep_table} r, {h_table} h, {table} p
                            where h.namespace = r.namespace
                                and h.name = r.name
                                and h.state = 'initial'
                                and p.id = h.project_id
                                and {p_wheres}
                    ),
                    available_files as 
                    (
                        select distinct ff.project_id, ff.namespace, ff.name, true as available
                            from found_files ff, {rep_table} r, {rse_table} s
                            where ff.namespace = r.namespace
                                and ff.name = r.name
                                and r.available
                                and s.name = r.rse and s.is_available
                    ),
                    handle_states as
                    (
                        select h.project_id, h.namespace, h.name, 
                                case
                                    when af.available = true then 'available'
                                    when ff.found = true then 'found'
                                    else h.state
                                end as state
                            from {table} p, {h_table} h
                                left outer join found_files ff on ff.namespace = h.namespace and ff.name = h.name
                                left outer join available_files af on af.namespace = h.namespace and af.name = h.name
                            where p.id = h.project_id
                    )
                    
                select {columns}, hs.state, count(*)
                    from handle_states hs, projects p
                        where p.id = hs.project_id
                        and {p_wheres}
                    group by p.id, hs.state
                    order by p.id, hs.state
            """
            c.execute(f"""
                with 
                    found_files as  
                    (
                        select distinct r.namespace, r.name, true as found
                            from {rep_table} r
                    ),
                    available_files as 
                    (
                        select distinct r.namespace, r.name, true as available
                            from {rep_table} r, {rse_table} s
                            where r.available and r.rse = s.name and s.is_available
                    ),
                    handle_states as
                    (
                        select h.project_id, h.namespace, h.name, 
                                case
                                    when h.state = 'initial' then (
                                        case
                                            when af.available = true then 'available'
                                            when ff.found = true then 'found'
                                            else 'not found'
                                        end
                                    )
                                    else h.state
                                end as state
                            from {table} p, {h_table} h
                                left outer join found_files ff on ff.namespace = h.namespace and ff.name = h.name
                                left outer join available_files af on af.namespace = h.namespace and af.name = h.name
                            where p.id = h.project_id
                    )
                    
                select {columns}, hs.state, count(*)
                    from handle_states hs, projects p
                        where p.id = hs.project_id
                        and {p_wheres}
                    group by p.id, hs.state
                    order by p.id, hs.state
            """)

            p = None
            for tup in cursor_iterator(c):
                #print(tup)
                p_tuple, h_state, count = tup[:len(DBProject.Columns)], tup[-2], tup[-1]
                p1 = DBProject.from_tuple(db, p_tuple)
                if p is None or p.ID != p1.ID:
                    if p is not None:
                        yield p
                    p = p1
                    p.HandleCounts = {}
                #print(p.ID, h_state, count)
                p.HandleCounts[h_state] = count
            if p is not None:
                yield p
        else:
            c.execute(f"""
                select {columns}
                    from {table} p
                    where {p_wheres}
            """)
            for tup in cursor_iterator(c):
                yield DBProject.from_tuple(db, tup)

    def save(self):
        c = self.DB.cursor()
        try:
            c.execute("begin")
            c.execute("""
                update projects set state=%s, end_timestamp=%s
                    where id=%s
            """, (self.State, self.EndTimestamp, self.ID))
            self.DB.commit()
        except:
            self.DB.rollback()
            raise

    def cancel(self):
        self.State = "cancelled"
        self.EndTimestamp = datetime.now(timezone.utc)
        self.save()
        self.add_log("state", event="cancel", state="cancelled")
        
    def restart_handles(self, states=None, dids=None):
        if states:
            states = set(states)
        if dids:
            dids = set(dids)
        handles_reset = []
        for h in self.handles(reload=True):
            if dids and h.did() in dids or \
                    states and h.State in states:
                h.reset()
                handles_reset.append(dict(did=h.did()))

        log_data = dict(event="restart",
            handles_reset=handles_reset
        )
        
        if states is not None:
            log_data["handle_states"] = list(states)
        if dids is not None:
            log_data["dids"] = list(dids)
        
        if self.State != "active" \
                and not all(h.State in ("done", "failed") for h in self.handles()):
            self.State = "active"
            log_data["state"] = self.State = "active"
            self.EndTimestamp = None
            self.save()
            self.add_log("state", log_data)
        else:
            self.add_log("event", log_data)

    def handles(self, state=None, with_replicas=True, reload=False):
        if reload or self.Handles is None:
            self.Handles = list(DBFileHandle.list(self.DB, project_id=self.ID, with_replicas=with_replicas))
        return (h for h in self.Handles if (state is None or h.State == state))

    def get_handles(self, dids, with_replicas=True):
        yield from DBFileHandle.get_bulk(self.DB, self.ID, dids, with_replicas=with_replicas)

    def handle(self, namespace, name):
        return DBFileHandle.get(self.DB, self.ID, namespace, name)

    def add_files(self, files_descs):
        # files_descs is list of disctionaries: [{"namespace":..., "name":...}, ...]
        files_descs = list(files_descs)     # make sure it's not a generator
        DBFile.create_many(self.DB, files_descs)
        DBFileHandle.create_many(self.DB, self.ID, files_descs)
        
    def files(self):
        return DBFile.list(self.DB, self.ID)
        
    def release_handle(self, namespace, name, failed, retry):
        handle = self.handle(namespace, name)
        if handle is None:
            return None

        if failed:
            handle.failed(retry)
        else:
            handle.done()

        if not self.is_active(reload=True) and self.State == "active":
            failed_handles = [h.did() for h in self.handles() if h.State == "failed"]

            if failed_handles:
                state = "failed"
                data = {"failed_handles": failed_handles}
            else:
                state = "done"
                data = {}

            self.add_log("state", event="release", state=state)

            self.State = state
            self.EndTimestamp = datetime.now(timezone.utc)
            self.save()
        return handle
        
    def is_active(self, reload=False):
        #print("projet", self.ID, "  handle states:", [h.State for h in self.handles(reload=reload)])
        p = self if not reload else DBProject.get(self.DB, self.ID)
        if p is None:   return False
        return p.State == "active" and not all(h.State in ("done", "failed") for h in p.handles())
            
    def ___reserve_handle(self, worker_id, proximity_map, cpu_site=None):

        if not self.is_active(reload=True):
            return None, "project inactive", False

        handles = sorted(
            self.handles(with_replicas=True, reload=True, state=DBFileHandle.ReadyState),
            key = lambda h: h.Attempts
        )
    
        #
        # find the lowest attempt count among all available file handles
        #
        
        handles_with_lowest_attempts = []
        lowest_attempts = None

        for h in handles:
            if lowest_attempts is not None and h.Attempts > lowest_attempts:
                break
            replicas = {}
            for r in h.replicas().values():
                if r.is_available():
                    rse = r.RSE
                    proximity = proximity_map.proximity(cpu_site, rse)
                    if proximity >= 0:
                        r.Preference = proximity
                        replicas[rse] = r

            if replicas:
                h.Replicas = replicas
                lowest_attempts = h.Attempts
                handles_with_lowest_attempts.append(h)

        #
        # reserve the most preferred handle
        #

        for h in sorted(handles_with_lowest_attempts, 
                    key=lambda h: min(r.Preference for r in h.Replicas.values())
                    ):
            if h.reserve(worker_id):
                return h, "ok", False

        return None, "retry", True

    def reserve_handle(self, worker_id, proximity_map, cpu_site):
        handle = DBFileHandle.reserve_for_worker(self.DB, self.ID, worker_id, proximity_map, cpu_site)
        if handle is not None:
            return handle, "ok", False
            
        if not self.is_active(reload=True):
            return None, "project inactive", False
        else:
            return None, "retry", True

    def file_state_counts(self):
        counts = {}
        for h in DBFileHandle.list(self.DB, project_id=self.ID):
            s = h.State
            counts[s] = counts.get(s, 0) + 1
        return out
        
    def project_log(self):
        return self.get_log()

    def handles_log(self):
        for log_record in DBFileHandle.log_records(self.DB, project_id=self.ID):
            log_record.Namespace = log_record.IDColumns["namespace"]
            log_record.Name = log_record.IDColumns["name"]
            yield log_record

    def files_log(self):
        return DBFile.log_records_for_project(self.DB, self.ID)
        
    @staticmethod
    def purge(db, retain=86400):        # 24 hours
        c = db.cursor()
        table = DBProject.Table
        t_retain = datetime.now(timezone.utc) - timedelta(seconds=retain)
        deleted = 0
        c.execute("begin")
        try:
            c.execute(f"""
                delete from {table}
                    where state in ('done', 'failed', 'cancelled')
                        and end_timestamp is not null
                        and end_timestamp < %s
            """, (t_retain,))
            deleted = c.rowcount
            c.execute("commit")
        except:
            c.execute("rollback")
            raise
        return deleted

    def replicas_logs(self):
        log_records = DBReplica.log_records_for_dids([(h.Namespace, h.Name) for h in self.handles()])
        out = {}
        for record in log_records:
            namespace = record.IDColumns["namespace"]
            name = record.IDColumns["name"]
            out.setdefault((namespace, name), []).append(record)
        for (namespace, name), lst in list(out.items()):
            out[(namespace, name)] = sorted(lst, key=lambda r: r.T)
        return out

    def release_timed_out_handles(self):
        if self.WorkerTimeout is None:
            return 0
        t = datetime.now() - timedelta(seconds=self.WorkerTimeout)
        return DBFileHandle.release_reserved_before(self.DB, self.ID, t)

class DBFile(DBObject):
    
    Columns = ["namespace", "name"]
    PK = ["namespace", "name"]
    Table = "files"
    
    #LogIDColumns = ["namespace", "name"]
    #LogTable = "file_log"
    
    def __init__(self, db, namespace, name):
        self.DB = db
        self.Namespace = namespace
        self.Name = name
        self.Replicas = None	# {rse -> DBReplica}
    
    def id(self):
        return f"{self.Namespace}:{self.Name}"

    def did(self):
        return f"{self.Namespace}:{self.Name}"

    def as_jsonable(self, with_replicas=False):
        out = dict(
            namespace   = self.Namespace,
            name        = self.Name,
        )
        if with_replicas:
            out["replicas"] = {rse: r.as_jsonable() for rse, r in self.replicas().items()}
        return out
        
    def replicas(self):
        if self.Replicas is None:
            self.Replicas = {r.RSE: r for r in DBReplica.list(self.DB, self.Namespace, self.Name)}
        return self.Replicas

    def create_replica(self, rse, path, url, preference=0, available=False):
        replica = DBReplica.create(self.DB, self.Namespace, self.Name, rse, path, url, preference=preference, available=available)
        self.Replicas = None	# force re-load from the DB
        return replica

    def get_replica(self, rse):
        return self.replicas().get(rse)
        
    @staticmethod
    def create(db, namespace, name, error_if_exists=False):
        c = self.DB.cursor()
        try:
            c.execute("begin")
            conflict = "on conflict (namespace, name) do nothing" if not error_if_exists else ""
            c.execute(f"insert into files(namespace, name) values(%s, %s) {conflict}; commit" % (namespace, name))
            return DBFile.get(db, namespace, name)
        except:
            c.execute("rollback")
            raise

    @staticmethod
    def create_many(db, descs):
        #
        # descs: [{"namespace":..., "name":..., ...}]
        #
        csv = [f"%s\t%s" % (item["namespace"], item["name"]) for item in descs]
        data = io.StringIO("\n".join(csv))
        table = DBFile.Table
        c = db.cursor()
        try:
            t = int(time.time()*1000)
            temp_table = f"files_temp_{t}"
            c.execute("begin")
            c.execute(f"create temp table {temp_table} (namespace text, name text)")
            c.copy_from(data, temp_table)
            c.execute(f"""
                insert into {table}(namespace, name)
                    select namespace, name from {temp_table}
                    on conflict(namespace, name) do nothing;
                drop table {temp_table};
                commit
                """)
        except Exception as e:
            c.execute("rollback")
            raise
            
    @staticmethod
    def list(db, project=None):
        project_id = project.ID if isinstance(project, DBProject) else project
        c = db.cursor()
        table = DBFile.Table
        columns = DBFile.columns(as_text=True)
        files_columns = DBFile.columns(table, as_text=True)
        project_where = f" id = {project_id} " if project is not None else " true "
        c.execute(f"""
            select {columns}
                from {table}, projects
                where {project_where}
        """)
        return (DBFile.from_tuple(db, tup) for tup in cursor_iterator(c))

    @staticmethod
    def delete_many(db, specs):
        csv = [f"{namespace}:{name}"  for namespace, name in specs]
        data = io.StringIO("\n".join(csv))
        
        c = db.cursor()
        try:
            t = int(time.time()*1000)
            temp_table = f"files_temp_{t}"
            c.execute("begin")
            c.execute(f"creare temp table {temp_table} (spec text)")
            c.copy_from(data, temp_table)
            c.execute(f"""
                delete from {self.Table} 
                    where namespace || ':' || name in 
                        (select * from {temp_table});
                    on conflict (namespace, name) do nothing;       -- in case some other projects still use it
                drop table {temp_table};
                commit
                """)
        except Exception as e:
            c.execute("rollback")
            raise
            
    @staticmethod
    def purge(db):
        c = db.cursor()
        table = DBFile.Table
        deleted = 0
        c.execute("begin")
        try:
            c.execute(f"""
                delete from {table} f
                    where not exists (
                            select * from file_handles h 
                                where f.namespace = h.namespace
                                    and f.name = h.name
                    )
            """)
            deleted = c.rowcount
            c.execute("commit")
        except:
            c.execute("rollback")
            raise
        return deleted


class DBReplica(DBObject, HasLogRecord):
    Table = "replicas"
    ViewWithRSEStatus = "replicas_with_rse_availability"
    
    Columns = ["namespace", "name", "rse", "path", "url", "preference", "available"]
    PK = ["namespace", "name", "rse"]
    
    LogIDColumns = ["namespace", "name", "rse"]
    LogTable = "replica_log"
    
    def __init__(self, db, namespace, name, rse, path, url, preference=0, available=None, rse_available=None):
        self.DB = db
        self.Namespace = namespace
        self.Name = name
        self.RSE = rse
        self.URL = url
        self.Path = path
        self.Preference = preference
        self.Available = available
        self.RSEAvailable = rse_available           # optional, set by joining the rses table
        self.Handle = None                          # back reference to the Handle object. Not used here. Just a placeholder

    def did(self):
        return f"{self.Namespace}:{self.Name}"
        
    def is_available(self):
        assert self.Available is not None and self.RSEAvailable is not None
        return self.Available and self.RSEAvailable

    @staticmethod
    def list(db, namespace=None, name=None, rse=None):
        c = db.cursor()
        wheres = " true "
        if namespace:   wheres += f" and namespace='{namespace}'"
        if name:        wheres += f" and name='{name}'"
        if rse:         wheres += f" and rse='{rse}'"
        columns = DBReplica.columns(as_text=True)
        table = DBReplica.Table
        c.execute(f"""
            select {columns}, rse_available from {DBReplica.ViewWithRSEStatus}
            where {wheres}
        """)
        for tup in cursor_iterator(c):
            r = DBReplica.from_tuple(db, tup[:-1])
            r.RSEAvailable = tup[-1]
            yield r
        
    @staticmethod
    def list_many_files(db, dids, rse=None):
        if dids:
            c = db.cursor()
            wheres = ""
            if rse:         wheres += f" and rse='{rse}'"
            columns = DBReplica.columns(as_text=True)
            table = DBReplica.Table

            c.execute(f"""
                select {columns}, rse_available from {DBReplica.ViewWithRSEStatus}
                where namespace || ':' || name = any(%s)
                        {wheres}
            """, (list(dids),))
            for tup in cursor_iterator(c):
                r = DBReplica.from_tuple(db, tup[:-1])
                r.RSEAvailable = tup[-1]
                yield r
        
    def as_jsonable(self):
        out = dict(name=self.Name, namespace=self.Namespace, path=self.Path, 
            url=self.URL, rse=self.RSE,
            preference=self.Preference, available=self.Available,
            rse_available=self.RSEAvailable
        )
        return out

    @staticmethod
    def create(db, namespace, name, rse, path, url, preference=0, available=False, error_if_exists=False):
        c = db.cursor()
        table = DBReplica.Table
        try:
            c.execute("begin")
            c.execute(f"""
                insert into {table}(namespace, name, rse, path, url, preference, available)
                    values(%s, %s, %s, %s, %s, %s, %s)
                    on conflict(namespace, name, rse)
                        do update set path=%s, url=%s, preference=%s, available=%s;
                commit
            """, (namespace, name, rse, path, url, preference, available,
                    path, url, preference, available)
            )
        except:
            c.execute("rollback")
            raise
        
        replica = DBReplica.get(db, namespace, name, rse)
        replica.add_log("state", {
            "event":        "create",
            "url":          url,
            "path":         path,
            "available":    available,
            "state":        "available" if available else "unavailable"
        })
        return replica

    def save(self):
        table = self.Table
        c = self.DB.cursor()
        try:
            c.execute(f"""
                begin;
                update {table}
                     set path=%s, url=%s, preference=%s, available=%s
                     where namespace=%s and name=%s and rse=%s;
                commit
            """, (self.Path, self.URL, self.Preference, self.Available, self.Namespace, self.Name, self.RSE))
        except:
            c.execute("rollback")
            raise
        return self

    #
    # bulk operations
    #
    
    @staticmethod
    def sync_replicas(db, by_namespace_name):
        # by_namespace_name: {(namespace, name) -> {rse: dict(path=path, url=url, available=available, preference=preference)}}
        # The input dictionary is presumed to have all the replicas found for (namespace, name). I.e. if the replica is not found
        # in the input dictionary, it should be deleted

        t = int(time.time()*1000)
        temp_table = f"replicas_temp_{t}"
        c = db.cursor()
        c.execute(f"""
            begin;
        """)

        records = []
        for (namespace, name), by_rse in by_namespace_name.items():
            for rse, info in by_rse.items():
                records.append((namespace, name, rse, info))


        csv = ['%s\t%s\t%s\t%s\t%s\t%s\t%s' % (namespace, name, rse, info["path"], info["url"], 'true' if info["available"] else 'false', info["preference"]) 
            for namespace, name, rse, info in records
        ]
        csv = io.StringIO("\n".join(csv))
        
        try:
            c.execute(f"""
                create temp table {temp_table}
                (
                    namespace   text,
                    name        text,
                    rse         text,
                    path        text,
                    url         text,
                    available   boolean,
                    preference  int
            )
            """)
            c.copy_from(csv, temp_table)
            
            #
            # delete replicas if (namespace, name, rse) not present in the list for (namespace, name)
            #
            c.execute(f"""
                delete from replicas r 
                    where row(r.namespace, r.name) in (select namespace, name from {temp_table})
                        and not row(r.namespace, r.name, r.rse) in (select namespace, name, rse from {temp_table});
            """)
            ndeleted = c.rowcount;
            
            #
            # insert new replicas and update existing ones
            #
            c.execute(f"""
                insert into replicas(namespace, name, rse, path, url, preference, available)
                    (
                        select namespace, name, rse, path, url, preference, available
                            from {temp_table}
                    )
                    on conflict(namespace, name, rse)
                    do update set path=excluded.path,
                        url=excluded.url,
                        preference=excluded.preference,
                        available=replicas.available or excluded.available
            """)
            c.execute(f"drop table {temp_table}")
            c.execute("commit")
        except:
            c.execute("rollback")
            traceback.print_exc()
            raise
    
    @staticmethod
    def remove_bulk(db, rse=None, dids=None):
        c = db.cursor()
        table = DBReplica.Table
        wheres = "true"
        if rse is not None: wheres += f" and rse='{rse}'"
        try:
            if dids is not None:
                c.execute(f"""
                    begin;
                    delete from {table}
                        where {wheres} and namespace || ':' || name = any(%s);
                """, (dids,))
            else:
                c.execute(f"""
                    begin;
                    delete from {table}
                        where {wheres};
                """)
            nremoved = c.rowcount
            c.execute("commit")
        except:
            c.execute("rollback")
            raise
        return nremoved

    @staticmethod
    def create_bulk(db, rse, available, preference, replicas):
        
        r = DBRSE.get(db, rse)
        if r is None or not r.Enabled:
            return
        
        # replicas: {(namespace, name) -> {"path":.., "url":..}}
        # do not touch availability, update if exists

        csv = ['%s\t%s\t%s\t%s\t%s\t%s' % (namespace, name, rse, info["path"], info["url"], preference) 
            for (namespace, name), info in replicas.items()]
        #print("DBReplica.create_bulk: csv:", csv)
        csv = io.StringIO("\n".join(csv))
        table = DBReplica.Table
        columns = DBReplica.columns(as_text=True)
        
        t = int(time.time()*1000)
        temp_table = f"file_replicas_temp_{t}"
        new_replicas = []
        c = db.cursor()
        try:
            c.execute("begin")
            c.execute(f"create temp table {temp_table} (ns text, n text, r text, p text, u text, pr int)")
            c.copy_from(csv, temp_table)
            csv = None         # to release memory
            c.execute(f"""
                insert into {table}({columns}) 
                    select t.ns, t.n, t.r, t.p, t.u, t.pr, %s from {temp_table} t
                    on conflict (namespace, name, rse)
                        do nothing
                    returning {table}.namespace, {table}.name
                """, (available,))
            new_replicas = c.fetchall()                     # new replicas only, for logging
            c.execute("commit")

        except Exception as e:
            c.execute("rollback")
            raise
        finally:
            try:
                c.execute("drop table {temp_table}")
            except:
                pass
        
        available_text = "available" if available else "unavailable"
        log_records = [
            (
                namespace_name + (rse,),
                "state",
                {
                    "event": "create",
                    "url":  replicas[namespace_name]["url"],
                    "path": replicas[namespace_name]["path"],
                    "available":    available,
                    "state":    available_text
                }
            )
            for namespace_name in new_replicas              # do not re-add "found" log records for existing replicas
        ]
        if log_records:
            DBReplica.add_log_bulk(db, log_records)
        return len(new_replicas)

    @staticmethod
    def update_availability_bulk(db, available, rse, dids):
        # dids is list of dids: ["namespace:name", ...]

        r = DBRSE.get(db, rse)
        if r is None or not r.Enabled:
            return
        
        if not dids:    return
        table = DBReplica.Table
        val = "true" if available else "false"
        updated = []
        c = db.cursor()
        c.execute("begin")
        try:
            sql = f"""
                update {table}
                    set available = %s
                    where namespace || ':' || name = any(%s)
                        and rse = %s
                        and available != %s
                    returning namespace, name
            """
            c.execute(sql, (val, dids, rse, val))
            updated = c.fetchall()
            c.execute("commit")
        except:
            c.execute("rollback")
            raise

        available_text = "available" if available else "unavailable"
        log_records = [
            (
                (namespace, name, rse),
                "state",
                { "available": available, "event": "update", "state": available_text }
            )
            for (namespace, name) in updated
        ]
        if log_records:
            DBReplica.add_log_bulk(db, log_records)

class DBFileHandle(DBObject, HasLogRecord):

    Columns = ["project_id", "namespace", "name", "state", "worker_id", "attempts", "attributes", "reserved_since"]
    PK = ["project_id", "namespace", "name"]
    Table = "file_handles"

    InitialState = ReadyState = "initial"
    ReservedState = "reserved"
    States = ["initial", "reserved", "done", "failed"]
    DerivedStates = [
            "not found",
            "found",
            "available", 
            "reserved",
            "done",
            "failed"
        ]

    LogIDColumns = ["project_id", "namespace", "name"]
    LogTable = "file_handle_log"

    def __init__(self, db, project_id, namespace, name, state=None, worker_id=None, attempts=0, attributes={}, reserved_since=None):
        self.DB = db
        self.ProjectID = project_id
        self.Namespace = namespace
        self.Name = name
        self.State = state or self.InitialState
        self.WorkerID = worker_id
        self.Attempts = attempts
        self.Attributes = (attributes or {}).copy()
        self.File = None
        self.Replicas = None
        self.ReservedSince = reserved_since

    def pk(self):
        return (self.ProjectID, self.Namespace, self.Name)

    def id(self):
        return f"{self.ProjectID}:{self.Namespace}:{self.Name}"
        
    @staticmethod
    def unpack_id(id):
        project_id, namespace, name = id.split(":", 2)
        return int(project_id), namespace, name

    def did(self):
        return f"{self.Namespace}:{self.Name}"

    def get_file(self):
        if self.File is None:
            self.File = DBFile.get(self.DB, self.Namespace, self.Name)
        return self.File
        
    def replicas(self):
        if self.Replicas is None:
            self.Replicas = self.get_file().replicas()
        return self.Replicas
        
    def state(self):
        # returns the handle state, including derived states like "available" and "found"
        if self.State == "initial":
            replicas = list(self.replicas().values())
            if replicas:
                if any(r.is_available() for r in replicas):
                    return "available"
                else:
                    return "found"
        return self.State
        
    def file_state(self):
        replicas = list(self.replicas().values())
        if replicas:
            if any(r.is_available() for r in replicas):
                return "available"
            else:
                return "found"
        else:
            return "not found"

    def sorted_replicas(self):
        replicas = self.replicas().values()
        return sorted(replicas,
            key = lambda r: (
                0 if r.Available else 1,
                -r.Preference
            )
        )

    def as_jsonable(self, with_replicas=False):
        out = dict(
            project_id = self.ProjectID,
            namespace = self.Namespace,
            name = self.Name,
            state = self.State,
            worker_id = self.WorkerID,
            attempts = self.Attempts,
            attributes = self.Attributes or {},
            reserved_since = self.ReservedSince.timestamp() if self.ReservedSince is not None else None
        )
        if with_replicas:
            out["replicas"] = {rse:r.as_jsonable() for rse, r in self.replicas().items()}
        return out
        
    def attributes_as_json(self):
        return json.dumps(self.Attributes, indent=4)
        
    @staticmethod
    def create(db, project_id, namespace, name, attributes={}):
        c = db.cursor()
        try:
            c.execute("begin")
            c.execute("""
                insert into file_handles(project_id, namespace, name, state, attempts, attributes)
                    values(%s, %s, %s, %s, 0, %s)
            """, (project_id, namespace, name, DBFileHandle.InitialState, json.dumps(attributes or {})))
            id = c.fetchone()[0]
            db.commit()
        except:
            c.execute("rollback")
            raise
        handle = DBFileHandle.get(db, project_id, namespace, name)
        handle.add_log("state", event="create", state="initial")
        return handle

    @staticmethod
    def create_many(db, project_id, files):
        #
        # files: [ {"name":"...", "namespace":"...", "attributes":{}}]
        #
        files_csv = []
        parents_csv = []
        null = r"\N"
        for info in files:
            namespace = info["namespace"]
            name = info["name"]
            attributes = info.get("attributes") or {}
            files_csv.append("%s\t%s\t%s\t%s\t%s" % (project_id, namespace, name, DBFileHandle.InitialState, json.dumps(attributes)))
        
        c = db.cursor()
        try:
            c.execute("begin")
            c.copy_from(io.StringIO("\n".join(files_csv)), "file_handles", 
                    columns = ["project_id", "namespace", "name", "state", "attributes"])
            c.execute("commit")
        except Exception as e:
            c.execute("rollback")
            raise
            
        log_records = [
            (
                (project_id, f["namespace"], f["name"]),
                "state",
                {
                    "state": "initial",
                    "event": "create"
                }
            )
            for f in files 
        ]            
        DBFileHandle.add_log_bulk(db, log_records)

    @staticmethod
    def get_bulk(db, project_id, dids, with_replicas=False):
        namespace_names = tuple(tuple(did.split(":", 1)) for did in dids)\x20\x20\x20\x20\x20\x20\x20\x23\x20\x6D\x75\x73\x74\x20\x62\x65\x20\x74\x75\x70\x6C\x65\x20\x6F\x66\x20\x74\x75\x70\x6C\x65\x73\x20\x66\x6F\x72\x20\x53\x51\x4C\x20\x74\x6F\x20\x77\x6F\x72\x6B\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x23\x70\x72\x69\x6E\x74\x28\x22\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x5F\x6E\x61\x6D\x65\x73\x3A\x22\x2C\x20\x74\x79\x70\x65\x28\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x5F\x6E\x61\x6D\x65\x73\x29\x2C\x20\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x5F\x6E\x61\x6D\x65\x73\x5B\x3A\x33\x5D\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x68\x5F\x63\x6F\x6C\x75\x6D\x6E\x73\x20\x3D\x20\x44\x42\x46\x69\x6C\x65\x48\x61\x6E\x64\x6C\x65\x2E\x63\x6F\x6C\x75\x6D\x6E\x73\x28\x22\x68\x22\x2C\x20\x61\x73\x5F\x74\x65\x78\x74\x3D\x54\x72\x75\x65\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x68\x5F\x6E\x5F\x63\x6F\x6C\x75\x6D\x6E\x73\x20\x3D\x20\x6C\x65\x6E\x28\x44\x42\x46\x69\x6C\x65\x48\x61\x6E\x64\x6C\x65\x2E\x43\x6F\x6C\x75\x6D\x6E\x73\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x72\x5F\x63\x6F\x6C\x75\x6D\x6E\x73\x20\x3D\x20\x44\x42\x52\x65\x70\x6C\x69\x63\x61\x2E\x63\x6F\x6C\x75\x6D\x6E\x73\x28\x22\x72\x22\x2C\x20\x61\x73\x5F\x74\x65\x78\x74\x3D\x54\x72\x75\x65\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x72\x5F\x6E\x5F\x63\x6F\x6C\x75\x6D\x6E\x73\x20\x3D\x20\x6C\x65\x6E\x28\x44\x42\x52\x65\x70\x6C\x69\x63\x61\x2E\x43\x6F\x6C\x75\x6D\x6E\x73\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x61\x76\x61\x69\x6C\x61\x62\x6C\x65\x5F\x72\x65\x70\x6C\x69\x63\x61\x73\x5F\x76\x69\x65\x77\x20\x3D\x20\x44\x42\x52\x65\x70\x6C\x69\x63\x61\x2E\x56\x69\x65\x77\x57\x69\x74\x68\x52\x53\x45\x53\x74\x61\x74\x75\x73\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x63\x20\x3D\x20\x64\x62\x2E\x63\x75\x72\x73\x6F\x72\x28\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x69\x66\x20\x77\x69\x74\x68\x5F\x72\x65\x70\x6C\x69\x63\x61\x73\x3A\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x73\x71\x6C\x20\x3D\x20\x66\x22\x22\x22\x5C\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x73\x65\x6C\x65\x63\x74\x20\x7B\x68\x5F\x63\x6F\x6C\x75\x6D\x6E\x73\x7D\x2C\x20\x7B\x72\x5F\x63\x6F\x6C\x75\x6D\x6E\x73\x7D\x2C\x20\x72\x73\x65\x5F\x61\x76\x61\x69\x6C\x61\x62\x6C\x65\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x66\x72\x6F\x6D\x20\x66\x69\x6C\x65\x5F\x68\x61\x6E\x64\x6C\x65\x73\x20\x68\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x69\x6E\x6E\x65\x72\x20\x6A\x6F\x69\x6E\x20\x7B\x61\x76\x61\x69\x6C\x61\x62\x6C\x65\x5F\x72\x65\x70\x6C\x69\x63\x61\x73\x5F\x76\x69\x65\x77\x7D\x20\x72\x20\x6F\x6E\x20\x28\x72\x2E\x6E\x61\x6D\x65\x20\x3D\x20\x68\x2E\x6E\x61\x6D\x65\x20\x61\x6E\x64\x20\x72\x2E\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x20\x3D\x20\x68\x2E\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x77\x68\x65\x72\x65\x20\x70\x72\x6F\x6A\x65\x63\x74\x5F\x69\x64\x20\x3D\x20\x25\x73\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x61\x6E\x64\x20\x28\x68\x2E\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x2C\x20\x68\x2E\x6E\x61\x6D\x65\x29\x20\x69\x6E\x20\x25\x73\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x6F\x72\x64\x65\x72\x20\x62\x79\x20\x68\x2E\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x2C\x20\x68\x2E\x6E\x61\x6D\x65\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x22\x22\x22\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x23\x70\x72\x69\x6E\x74\x28\x22\x44\x42\x46\x69\x6C\x65\x48\x61\x6E\x64\x6C\x65\x2E\x6C\x69\x73\x74\x3A\x20\x73\x71\x6C\x3A\x22\x2C\x20\x73\x71\x6C\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x70\x72\x69\x6E\x74\x28\x63\x2E\x6D\x6F\x67\x72\x69\x66\x79\x28\x73\x71\x6C\x2C\x20\x28\x70\x72\x6F\x6A\x65\x63\x74\x5F\x69\x64\x2C\x20\x6E\x61\x6D\x65\x73\x70\x61\x63\x65\x5F\x6E\x61\x6D\x65\x73\x29\x29\x2E\x64\x65\x63\x6F\x64\x65\x28\x22\x75\x74\x66\x2D\x38\x22\x29\x29\x0A\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20\x63\x2E\x65\x78\x65\x63\x75\x74\x65\x28\x73\x71\x6C\x2 