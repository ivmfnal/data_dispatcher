import json, time, io, traceback, urllib.parse
from datetime import datetime, timedelta, timezone
from metacat.auth import BaseDBUser as DBUser, BaseDBRole as DBRole

def cursor_iterator(c):
    t = c.fetchone()
    while t is not None:
        yield t
        t = c.fetchone()

def json_literal(v):
    if isinstance(v, str):       v = '"%s"' % (v.replace("'", "''"),)
    elif isinstance(v, bool):    v = "true" if v else "false"
    elif v is None:              v = "null"
    else:   v = str(v)
    return v
    
def to_timedelta(t):
    if isinstance(t, (int, float)):
        t = timedelta(seconds=t)
    return t
    
class HasDB(object):
    
    Table = None
    Columns = None
    PK = None

    # instances must have DB attribute
    
    def __init__(self):
        self.DB = None

def transactioned(method):
    def decorated(first, *params, transaction=None, **args):

        if transaction is not None:
            return method(first, *params, transaction=transaction, **args)

        if isinstance(first, HasDB):
            transaction = first.DB.transaction()
        elif isinstance(first, type):
            # class method -- DB is second argument
            transaction = params[0].transaction()
        else:
            transaction = first.transaction()       # static method

        with transaction:
            return method(first, *params, transaction=transaction, **args)

    return decorated

def sanitize(s):
    s = str(s)
    if "'" in s:
        raise ValueError('Invalid value: "%s"' % (s,))

class DBObject(HasDB):
    
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
        where_clause = " and ".join("%s = '%s'" % (column, sanitize(value),) for column, value in pk_values.items())
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
        pk_values = dict(zip(self.PK, self.pk()))
        return self._delete(cursor=None, do_commit=True, **pk_values)
    
class DBManyToMany(HasDB):
    
    def __init__(self, db, table, src_fk_values, dst_fk_columns, payload_columns, dst_class):
        self.DB = db
        self.Table = table
        self.SrcFKColumns, self.SrcFKValues = zip(*list(src_fk_values.items()))
        self.SrcFKColumns = list(self.SrcFKColumns)
        self.SrcFKValues = list(self.SrcFKValues)
        self.DstFKColumns = dst_fk_columns
        self.DstClass = dst_class
        self.DstTable = dst_class.Table
        self.DstPKColumns = dst_class.PK
        self.PayloadColumns = payload_columns

    @transactioned
    def add(self, dst_pk_values, payload={}, transaction=None):
        assert len(dst_pk_values) == len(self.DstFKColumns)
        
        payload_cols_vals = list(payload.items())
        payload_cols, payload_vals = [], []
        if payload:
            payload_cols, payload_vals = zip(*payload_cols_vals)
        
        fk_cols = ",".join(self.SrcFKColumns + self.DstFKColumns)
        cols = ",".join(self.SrcFKColumns + self.DstFKColumns + payload_cols)
        values = tuple(self.SrcFKValues + dst_pk_values + payload_vals)
        value_placeholders = ",".join(["%s"] * len(values))
        
        transaction.execute(f"""
                insert into {self.Table}({cols}) values({value_placeholders})
                    on conflict({fk_cols}) do nothing
            """, values)
        return self

    @transactioned
    def list(self, transaction=None):
        out_columns = ",".join(f"{self.DstTable}.{c}" for c in self.DstClass.Columns)
        join_column_pairs = [
            (f"{self.Table}.{dst_fk}", f"{self.DstTable}.{dst_pk}") 
            for dst_fk, dst_pk in zip(self.DstFKColumns, self.DstPKColumns)
        ]
        src_fk_conditions = " and ".join([f"{c} = %s" for c in self.SrcFKColumns])
        join_conditions = " and ".join(f"{fk} = {pk}" for fk, pk in join_column_pairs)
        transaction.execute(f"""
            select {out_columns}
                from {self.DstTable}, {self.Table}
                where {src_fk_conditions} and {join_conditions}
        """, tuple(self.SrcFKValues))
        return (self.DstClass.from_tuple(self.DB, tup) for tup in cursor_iterator(transaction))
        
    def __iter__(self):
        return self.list()

class DBOneToMany(HasDB):
    
    def __init__(self, db, table, src_pk_values, dst_fk_columns, dst_class):
        self.DB = db
        self.Table = table
        self.SrcPKColumns, self.SrcPKValues = zip(*list(src_pk_values.items()))
        self.DstClass = dst_class
        self.DstTable = dst_class.Table
        self.DstFKColumns = dst_fk_columns

    @transactioned
    def list(self, transaction=None):
        out_columns = ",".join(f"{self.DstTable}.{c}" for c in self.DstClass.Columns)
        join_column_pairs = [
            (f"{self.Table}.{src_pk}", f"{self.DstTable}.{dst_fk}") 
            for src_pk, dst_fk in zip(self.SrcPKColumns, self.DstFKColumns)
        ]
        join_condition = " and ".join(f"{pk} = {fk}" for pk, fk in join_column_pairs)
        transaction.execute(f"""
            select {out_columns}
                from {self.DstTable}, {self.Table}
                where {join_condition}
        """)
        return (self.DstClass.from_tuple(self.DB, tup) for tup in cursor_iterator(transaction))
        
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

    LogIDColumns = None
    LogTable = None

    @transactioned
    def add_log(self, type, data=None, transaction=None, **kwargs):
        #print("add_log:", type, data, kwargs)
        data = (data or {}).copy()
        data.update(kwargs)
        parent_pk_columns = ",".join(self.LogIDColumns)
        parent_pk_values = ",".join([f"'{v}'" for v in self.pk()])
        transaction.execute(f"""
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
    @transactioned
    def add_log_bulk(cls, db, records, transaction=None):
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
            transaction.copy_from(csv, table, columns=columns)

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
        
    def last_log_record(self, type=None):
        parent_pk_columns = self.LogIDColumns
        c = self.DB.cursor()
        pk_wheres = " and ".join([f"({c} = %s)" for c in parent_pk_columns])
        c.execute(f"""
            select type, t, data from {self.LogTable}
                where {pk_wheres}
                    and (%s is null or type = %s)
                order by t desc
                limit 1
        """, tuple(self.pk()) + (type, type)
        )
        tup = c.fetchone()
        if tup:
            return DBLogRecord(*tup)
        else:
            return None

class DBProject(DBObject, HasLogRecord):
    
    InitialState = "active"
    States = ["active", "failed", "done", "cancelled", "held", "abandoned"]
    EndStates = ["failed", "done", "cancelled"]
    
    Columns = "id,owner,created_timestamp,end_timestamp,state,retry_count,attributes,query,worker_timeout,idle_timeout".split(",")
    Table = "projects"
    PK = ["id"]
    
    LogIDColumns = ["project_id"]
    LogTable = "project_log"
    
    def __init__(self, db, id, owner=None, created_timestamp=None, end_timestamp=None, state=None, 
                retry_count=None, attributes={}, query=None, worker_timeout=None, idle_timeout=None):
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
        self.WorkerTimeout = to_timedelta(worker_timeout)
        self.IdleTimeout = to_timedelta(idle_timeout)
        self.Users = DBManyToMany(db, "project_users", {"project_id":id}, ["username"], [], DBUser)    # if other than the owner
        self.Roles = DBManyToMany(db, "project_roles", {"project_id":id}, ["role_name"], [], DBRole)

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
            worker_timeout = None if self.WorkerTimeout is None else self.WorkerTimeout.total_seconds(),
            idle_timeout = None if self.IdleTimeout is None else self.IdleTimeout.total_seconds(),
            users = [u.Username for u in self.Users],
            roles = [r.Name for r in self.Roles]
        )
        if with_handles or with_replicas:
            out["file_handles"] = [h.as_jsonable(with_replicas=with_replicas) for h in self.handles()]
            #print("Project.as_jsonable: handles:", out["file_handles"])
        return out

    def attributes_as_json(self):
        return json.dumps(self.Attributes, indent=4)
        
    @staticmethod
    def create_notxn(db, owner, retry_count=None, attributes={}, query=None, worker_timeout=None, idle_timeout=None):
        if isinstance(owner, DBUser):
            owner = owner.Username
        c = db.cursor()
        try:
            c.execute("begin")
            c.execute("""
                insert into projects(owner, state, retry_count, attributes, query, worker_timeout, idle_timeout)
                    values(%s, %s, %s, %s, %s, %s, %s)
                    returning id
            """, (owner, DBProject.InitialState, retry_count, json.dumps(attributes or {}), query, 
                        to_timedelta(worker_timeout), to_timedelta(idle_timeout))
            )
            id = c.fetchone()[0]
            db.commit()
        except:
            db.rollback()
            raise
            
        project = DBProject.get(db, id)
        return project

    def handle_states(self):
        return {(h.Namespace, h.Name): h.Availability if h.State == "initial" else h.State 
                for h in self.handles(reload=True, with_availability=True)}

    @staticmethod
    @transactioned
    def create(db, owner, retry_count=None, attributes={}, query=None, worker_timeout=None, idle_timeout=None,
                    transaction=None, users=[], roles=[]):
        if isinstance(owner, DBUser):
            owner = owner.Username

        transaction.execute("""
            insert into projects(owner, state, retry_count, attributes, query, worker_timeout, idle_timeout)
                values(%s, %s, %s, %s, %s, %s, %s)
                returning id
        """, (owner, DBProject.InitialState, retry_count, json.dumps(attributes or {}), query, 
                    to_timedelta(worker_timeout), to_timedelta(idle_timeout))
        )
        id = transaction.fetchone()[0]

        project = DBProject.get(db, id)
        
        if users:
            project.add_users(users)
        if roles:
            project.add_roles(roles)

        return project

    def add_users(self, users):
        current_users = set(u.Username for u in self.users)
        for u in users:
            if u not in current_users:
                self.Users.add([u])
                
    def add_roles(self, roles):
        current_roles = set(r.Name for r in self.roles)
        for r in roles:
            if r not in current_roles:
                self.Roles.add([r])

    @property
    def users(self):
        return sorted(list(self.Users), key=lambda u: u.Username)

    @property
    def roles(self):
        return sorted(list(self.Roles), key=lambda r: r.Name)
        
    def authorized_user(self, username):
        return username == self.Owner or any(u.Username == username for u in self.users) or \
            any(username in r for r in self.roles)

    @staticmethod
    def from_sql(db, sql):
        c = db.cursor()
        c.execute(sql)
        return (DBProject.from_tuple(db, tup) for tup in cursor_iterator(c))

    @staticmethod
    def list(db, owner=None, state=None, not_state=None, attributes=None, with_handle_counts=False):
        wheres = [
            "(%(owner)s is null or p.owner=%(owner)s)",
            "(%(state)s is null or p.state=%(state)s)",
            "(%(not_state)s is null or p.state!=%(not_state)s)"
        ]
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
            """, {"state":state, "not_state":not_state, "owner":owner})

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
            """, {"state":state, "not_state":not_state, "owner":owner})
            for tup in cursor_iterator(c):
                yield DBProject.from_tuple(db, tup)

    def save_old(self):
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

    @transactioned
    def save(self, transaction=None):
        transaction.execute("""
            update projects set state=%s, end_timestamp=%s
                where id=%s
        """, (self.State, self.EndTimestamp, self.ID))

    @transactioned
    def cancel(self, transaction=None):
        if self.State != "cancelled":
            self.State = "cancelled"
            self.EndTimestamp = datetime.now(timezone.utc)
            self.save(transaction=transaction)
            self.add_log("state", event="cancel", state="cancelled", transaction=transaction)

    @transactioned
    def activate(self, transaction=None):
        if self.State != "active":
            self.State = "active"
            self.save(transaction=transaction)
            self.add_log("state", event="activate", state="active", transaction=transaction)

    @transactioned
    def restart_handles(self, states=None, dids=None, transaction=None):
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
            self.save(transaction=transaction)
            self.add_log("state", log_data, transaction=transaction)
        else:
            self.add_log("event", log_data, transaction=transaction)

    def handles(self, state=None, with_replicas=True, with_availability=True, reload=False):
        if reload or self.Handles is None:
            #print("DBProject.handles(): getting handles with_replicas:", with_replicas, " with_availability:", with_availability)
            self.Handles = list(DBFileHandle.list(self.DB, project_id=self.ID, 
                with_replicas=with_replicas, with_availability=with_availability)
            )
            #print("received", len(self.Handles))
        return (h for h in self.Handles if (state is None or h.State == state))

    def get_handles(self, dids, with_replicas=True):
        yield from DBFileHandle.get_bulk(self.DB, self.ID, dids, with_replicas=with_replicas)

    def handle(self, namespace, name):
        return DBFileHandle.get(self.DB, self.ID, namespace, name)
        
    def add_files(self, files_descs):
        # files_descs is list of disctionaries: [{"namespace":..., "name":...}, ...]
        files_descs = list(files_descs)     # make sure it's not a generator
        DBFileHandle.create_many(self.DB, self.ID, files_descs)
        
    @transactioned
    def reserve_handle(self, worker_id, transaction=None):
        handle = DBFileHandle.reserve_for_worker(self.DB, self.ID, worker_id, transaction=transaction)
        if handle is not None:
            return handle, "ok", False
            
        if not self.is_active(reload=True):
            return None, "project inactive", False
        else:
            return None, "retry", True

    @transactioned
    def release_handle(self, namespace, name, failed, retry, transaction=None):
        handle = self.handle(namespace, name)
        if handle is None or handle.State != "reserved":
            return None

        if failed:
            handle.failed(retry, transaction=transaction)
        else:
            handle.done(transaction=transaction)

        if self.State == "active" and not self.is_active(reload=True):
            failed_handles = [h.did() for h in self.handles() if h.State == "failed"]

            if failed_handles:
                state = "failed"
                data = {"failed_handles": failed_handles}
            else:
                state = "done"
                data = {}

            self.add_log("state", event="release", state=state, transaction=transaction)

            self.State = state
            self.EndTimestamp = datetime.now(timezone.utc)
            self.save(transaction=transaction)
        return handle
        
    def is_active(self, reload=False):
        #print("projet", self.ID, "  handle states:", [h.State for h in self.handles(reload=reload)])
        p = self if not reload else DBProject.get(self.DB, self.ID)
        if p is None:   return False
        return p.State == "active" and not all(h.State in ("done", "failed") for h in p.handles())

    def file_state_counts(self):
        counts = {}
        for h in DBFileHandle.list(self.DB, project_id=self.ID):
            s = h.State
            counts[s] = counts.get(s, 0) + 1
        return counts
        
    def project_log(self):
        return self.get_log()

    def handles_log(self):
        for log_record in DBFileHandle.log_records(self.DB, project_id=self.ID):
            log_record.Namespace = log_record.IDColumns["namespace"]
            log_record.Name = log_record.IDColumns["name"]
            yield log_record

    @staticmethod
    @transactioned
    def find_abandoned(db, transaction=None):
        p_table = DBProject.Table
        hl_table = DBFileHandle.LogTable
        pl_table = DBProject.LogTable
        columns = DBProject.columns("p", as_text=True)
        transaction.execute(f"""\
            select {columns}, max(hl.t), max(pl.t)
                from {p_table} p
                    left outer join {hl_table} hl on (hl.project_id=p.id)
                    left outer join {pl_table} pl on (pl.project_id=p.id)
                where p.idle_timeout is not null
                    and p.state = 'active'
                    and p.created_timestamp + p.idle_timeout < now()
                group by p.id
                having (max(hl.t) is null or max(hl.t) + p.idle_timeout < now())
                    and (max(pl.t) is null or max(pl.t) + p.idle_timeout < now())
        """)
        return (DBProject.from_tuple(db, tup[:-2]) for tup in transaction.cursor_iterator())

    @staticmethod
    @transactioned
    def mark_abandoned(db, transaction=None):
        # assume every handle has at least one log record on creation
        project_ids = [p.ID for p in DBProject.find_abandoned(db, transaction=transaction)]
        n = 0
        if project_ids:
            p_table = DBProject.Table
            r_table = DBReplica.Table
            transaction.execute(f"""\
                update {p_table} p 
                    set state='abandoned'
                    where p.id = any(%s)
                """, (project_ids,))
            n = transaction.rowcount
        return n
        
    @staticmethod
    def purge_old(db, retain=86400):        # 24 hours
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

    @transactioned
    @staticmethod
    def purge(db, retain=86400, transaction=None):        # 24 hours
        table = DBProject.Table
        t_retain = datetime.now(timezone.utc) - timedelta(seconds=retain)
        deleted = 0
        transaction.execute(f"""
                delete from {table}
                    where state in ('done', 'failed', 'cancelled')
                        and end_timestamp is not null
                        and end_timestamp < %s
            """, (t_retain,))
        return transaction.rowcount

    def release_timed_out_handles(self):
        if self.WorkerTimeout is None:
            return 0
        t = datetime.now() - to_timedelta(self.WorkerTimeout)
        return DBFileHandle.release_reserved_before(self.DB, self.ID, t)

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

    def did(self):
        return f"{self.Namespace}:{self.Name}"
        
    def is_available(self):
        if self.Available is not None and self.RSEAvailable is not None:
            return self.Available and self.RSEAvailable
        else:
            return None

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
    @transactioned
    def create(db, namespace, name, rse, path, url, preference=0, available=False, error_if_exists=False, transaction=None):
        table = DBReplica.Table
        transaction.execute(f"""
            insert into {table}(namespace, name, rse, path, url, preference, available)
                values(%s, %s, %s, %s, %s, %s, %s)
                on conflict(namespace, name, rse)
                    do update set path=%s, url=%s, preference=%s, available=%s;
            commit
        """, (namespace, name, rse, path, url, preference, available,
                path, url, preference, available)
        )
        
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
            
    @staticmethod
    @transactioned
    def purge(db, transaction=None):
        table = DBReplica.Table
        h_table = DBFileHandle.Table
        p_table = DBProject.Table
        transaction.execute(f"""\
            delete from {table} r
                where not exists (
                    select * from {h_table} h
                        where r.namespace = h.namespace and r.name = h.name
                )
        """)
        norphans = transaction.rowcount
        
        transaction.execute(f"""\
            delete from {table} r
                where not exists (
                    select pp.id
                        from {p_table} pp, {h_table} hh
                        where pp.state = 'active'
                            and pp.id = hh.project_id
                            and hh.namespace = r.namespace and hh.name = r.name
                )
        """)
        nabandoned = transaction.rowcount
        return nabandoned + norphans

class DBFileHandle(DBObject, HasLogRecord):

    Columns = ["project_id", "namespace", "name", "state", "worker_id", "attempts", "attributes", "reserved_since"]
    PK = ["project_id", "namespace", "name"]
    Table = "file_handles"

    InitialState = ReadyState = "initial"
    ReservedState = "reserved"
    States = ["initial", "reserved", "done", "failed"]
    DerivedStates = [
            "available", 
            "reserved",
            "not found",
            "found",
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
        self.Availability = None             # "not found", "found" (but unavailable), "available"
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

    def replicas(self):
        if self.Replicas is None:
            self.Replicas = {r.RSE:r for r in DBReplica.list(self.DB, self.Namespace, self.Name)}
        return self.Replicas
        
    def state(self):
        # returns conbined handle state, including derived states like "available" and "found"
        if self.State == "initial":
            replicas = list(self.replicas().values())
            if replicas:
                if any(r.is_available() for r in replicas):
                    return "available"
                else:
                    return "found"
            else:
                return "not found"
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
    @transactioned
    def create_many(db, project_id, files, transaction=None):
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
        
        transaction.copy_from(io.StringIO("\n".join(files_csv)), "file_handles", 
                    columns = ["project_id", "namespace", "name", "state", "attributes"])
            
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
        DBFileHandle.add_log_bulk(db, log_records, transaction=transaction)

    @staticmethod
    def get_bulk(db, project_id, dids, with_replicas=False):
        #print("namespace_names:", type(namespace_names), namespace_names[:3])
        dids = list(dids)
        h_columns = DBFileHandle.columns("h", as_text=True)
        h_n_columns = len(DBFileHandle.Columns)
        r_columns = DBReplica.columns("r", as_text=True)
        r_n_columns = len(DBReplica.Columns)
        available_replicas_view = DBReplica.ViewWithRSEStatus
        c = db.cursor()
        if with_replicas:
            sql = f"""\
                select {h_columns}, {r_columns}, rse_available
                    from file_handles h
                        inner join {available_replicas_view} r on (
                            (r.namespace || ':' || r.name) = (h.namespace || ':' || h.name)
                        )
                        where h.project_id = %s
                            and (h.namespace || ':' || h.name) = any(%s)
                        order by h.namespace, h.name
            """
            #print("DBFileHandle.list: sql:", sql)
            c.execute(sql, (project_id, dids))
            h = None
            for tup in cursor_iterator(c):
                #print("DBFileHandle.get_bulk:", tup)
                h_tuple, r_tuple, rse_available = tup[:h_n_columns], tup[h_n_columns:h_n_columns+r_n_columns], tup[-1]
                if h is None:
                    h = DBFileHandle.from_tuple(db, h_tuple)
                #print("DBFileHandle.get_bulk: h:", h)
                h1 = DBFileHandle.from_tuple(db, h_tuple)
                if h1.Namespace != h.Namespace or h1.Name != h.Name:
                    if h:   
                        #print("    yield:", h)
                        yield h
                    h = h1
                if r_tuple[0] is not None:
                    r = DBReplica.from_tuple(db, r_tuple)
                    r.RSEAvailable = rse_available
                    h.Replicas = h.Replicas or {}
                    h.Replicas[r.RSE] = r
            if h is not None:
                #print("    yield:", h)
                yield h
        else:
            sql = f"""
                select {h_columns}
                    from file_handles h
                        where project_id = %s and (namespace || ':' || name) = any(%s)
                        order by h.namespace, h.name
            """
            c.execute(sql, (project_id, dids))
            yield from (DBFileHandle.from_tuple(db, tup) for tup in cursor_iterator(c))

    @staticmethod
    def list(db, project_id=None, state=None, namespace=None, not_state=None, with_replicas=False, with_availability=False):
        wheres = []
        if project_id: wheres.append(f"h.project_id={project_id}")
        if state:
            if isinstance(state, (list, tuple)):
                wheres.append("h.state in (%s)" % ",".join(f"'{s}'" for s in state))
            else:
                wheres.append(f"h.state='{state}'")
        if not_state:       wheres.append(f"h.state!='{not_state}'")
        if namespace:       wheres.append(f"h.namespace='{namespace}'")
        wheres = " and ".join(wheres)
        c = db.cursor()
        h_columns = DBFileHandle.columns("h", as_text=True)
        r_columns = DBReplica.columns("r", as_text=True)
        h_n_columns = len(DBFileHandle.Columns)
        r_n_columns = len(DBReplica.Columns)
        available_replicas_view = DBReplica.ViewWithRSEStatus
        if with_replicas:
            if with_availability:
                sql  = f"""\
                    select {h_columns}, {r_columns}, r.rse_available
                        from file_handles h
                            left outer join 
                            (   select rr.*, rs.is_available as rse_available
                                    from replicas rr, rses rs
                                        where rr.rse = rs.name
                            ) r on (r.name = h.name and r.namespace = h.namespace)
                        where (%(project_id)s is null or h.project_id = %(project_id)s)
                        order by h.project_id, h.namespace, h.name
                    """
            else:
                sql  = f"""\
                    select {h_columns}, {r_columns}, null
                        from file_handles h
                            left outer join replicas r on (r.name = h.name and r.namespace = h.namespace)
                        where (%(project_id)s is null or h.project_id = %(project_id)s)
                        order by h.project_id, h.namespace, h.name
                    """
            c.execute(sql, {"project_id":project_id})
            #print("DBFileHandle.list: sql:", sql)
            h = None
            for tup in cursor_iterator(c):
                #print("DBFileHandle.list:", tup)
                h_tuple, r_tuple, rse_available = tup[:h_n_columns], tup[h_n_columns:h_n_columns+r_n_columns], tup[-1]
                #print("Handle.list: tuples:", h_tuple, r_tuple, rse_available)
                h1 = DBFileHandle.from_tuple(db, h_tuple)
                if h is None or h1.Namespace != h.Namespace or h1.Name != h.Name:
                    if h is not None:
                        yield h
                    h = h1
                    h.Replicas = {}
                    h.Availability = "not found" if with_availability else None
                if r_tuple[0] is not None:
                    if h.Availability != "available":
                        h.Availability = "found"
                    r = DBReplica.from_tuple(db, r_tuple)
                    r.RSEAvailable = bool(rse_available)
                    h.Replicas[r.RSE] = r
                    if with_availability and rse_available and r.Available:
                        h.Availability = "available"
            if h is not None:
                #print("    yield:", h, len(h.Replicas))
                yield h
        else:
            sql = f"""
                select {h_columns}
                    from file_handles h
                        where {wheres}
            """
            c.execute(sql)
            yield from (DBFileHandle.from_tuple(db, tup) for tup in cursor_iterator(c))
    
    
    @transactioned
    def save(self, transaction=None):
        transaction.execute("""
                update file_handles set state=%s, worker_id=%s, attempts=%s, attributes=%s
                    where project_id=%s and namespace=%s and name=%s
            """, (self.State, self.WorkerID, self.Attempts, json.dumps(self.Attributes), 
                    self.ProjectID, self.Namespace, self.Name
                )            
            )
            
    @property
    def project(self):
        return DBProject.get(self.DB, self.ProjectID)

    @staticmethod
    @transactioned
    def reserve_for_worker(db, project_id, worker_id, transaction=None):
        h_table = DBFileHandle.Table
        rep_table = DBReplica.Table
        rse_table = DBRSE.Table
        reserved = None
        sql = f"""
                select h.namespace, h.name
                    from {h_table} h
                    where 
                        h.project_id = %s and h.state = %s
                        and exists (
                            select * from {rep_table} r, {rse_table} s
                                where h.namespace = r.namespace and h.name = r.name 
                                    and r.rse = s.name
                                    and s.is_enabled and s.is_available
                        )
                    order by attempts
                    limit 1
                    for update skip locked
        """
        #print("sql:\n", sql)
        transaction.execute(sql, (project_id, DBFileHandle.ReadyState))
        tup = transaction.fetchone()
        if tup:
            namespace, name = tup
            transaction.execute(f"""
                update {h_table}
                    set state = %s, worker_id = %s, attempts = attempts + 1, reserved_since = now()
                    where project_id = %s and namespace = %s and name = %s
            """, (DBFileHandle.ReservedState, worker_id, project_id, namespace, name))
            reserved = (namespace, name)

        if reserved:
            namespace, name = reserved
            reserved = DBFileHandle.get(db, project_id, namespace, name)
            reserved.record_state_change(DBFileHandle.ReservedState, 
                event = "reserve",
                old_state = DBFileHandle.ReadyState, worker=worker_id,
                transaction=transaction)
        return reserved
        
    #
    # workflow
    #

    @staticmethod
    @transactioned
    def release_reserved_before(db, project_id, reserved_before, transaction=None):
        if reserved_before is None:
            return 0
        transaction.execute("""
            update file_handles h_new
                set state = %s, worker_id = null
                from file_handles h_old                     -- this is the trick to get the worker_id before it is updated to null
                where h_new.project_id = %s and h_new.state = %s and h_new.reserved_since < %s
                    and h_new.project_id = h_old.project_id and h_new.namespace = h_old.namespace and h_new.name = h_old.name
                returning h_new.namespace, h_new.name, h_old.worker_id
        """, (DBFileHandle.ReadyState, project_id, DBFileHandle.ReservedState, reserved_before))
        log_records = [
            (
                (project_id, namespace, name),
                "state",
                dict(event = "worker_timeout", state=DBFileHandle.ReadyState, worker=worker_id)
            ) for namespace, name, worker_id in transaction.fetchall()
        ]
        DBFileHandle.add_log_bulk(db, log_records, transaction=transaction)
        return len(log_records)

    def is_available(self):
        return any(r.Available and r.RSEAvailable for r in self.replicas().values())

    def is_active(self):
        return self.State not in ("done", "failed")
        
    def is_reserved(self):
        return self.State == self.ReservedState

    @transactioned
    def record_state_change(self, new_state, old_state=None, transaction=None, **log_data):
        assert new_state in self.States, "Unknown file handle state: "+new_state
        old_state = old_state or self.State
        self.State = new_state
        data = log_data.copy()
        data["state"] = new_state
        data["old_state"] = old_state
        self.add_log("state", data, transaction=transaction)

    @transactioned
    def set_state(self, state, transaction=None, **log_data):
        assert state in self.States, "Unknown file handle state: " + state
        if self.State != state:
            self.record_state_change(state, transaction=transaction, **log_data)
            self.State = state
            
    @transactioned
    def done(self, transaction=None):
        self.set_state("done", event="done", worker=self.WorkerID, transaction=transaction)
        self.WorkerID = None
        self.save()

    @transactioned
    def failed(self, retry=True, transaction=None):
        state = self.ReadyState if retry else "failed"
        self.set_state(state, event="failed", worker=self.WorkerID, transaction=transaction)
        self.WorkerID = None
        self.save(transaction=transaction)
        
    @transactioned
    def reset(self, transaction=None):
        self.set_state(self.ReadyState, event="reset", worker=self.WorkerID, transaction=transaction)
        self.WorkerID = None
        self.save(transaction=transaction)

    LOG_EVENTS = "reserve,worker_timeout,reset,failed,done,create".split(",")

    @staticmethod
    def event_counts(db, t0, bin):
        t_begin = datetime.fromtimestamp(t0, timezone.utc)
        t1 = time.time()
        t1 = int((t1 + bin - 1)/bin) * bin
        t_end = datetime.fromtimestamp(t1, timezone.utc)
        c = db.cursor()
        c.execute(f"""
            select (extract(epoch from t)/{bin}::int)*{bin}, data->'event', count(*) 
                from file_handle_log 
                where t >= %s and t < %s
                group by data->'event', (extract(epoch from t)/{bin}::int)*{bin}
                order by data->'event', (extract(epoch from t)/{bin}::int)*{bin};
        """, (t_begin, t_end))
        n_times = int((t1 - t0)/bin)
        zeros = [0] * n_times
        counts = {
            event: zeros[:] for event in DBFileHandle.LOG_EVENTS
        }
        for t, event, count in c.fetchall():
            i = int((float(t)-t0)/bin)
            counts[event][i] += 1
        return t0, t1, DBFileHandle.LOG_EVENTS, counts

class DBRSE(DBObject):
    
    Columns = ["name", "description", "is_enabled", "is_available", "is_tape", "pin_url", "poll_url", "remove_prefix", "add_prefix", "pin_prefix", "preference", "type"]
    PK = ["name"]
    Table = "rses"

    def __init__(self, db, name, description="", is_enabled=False, is_available=True, is_tape=False, pin_url=None, poll_url=None, 
                remove_prefix=None, add_prefix=None, pin_prefix=None, preference=0, type=None):
        self.DB = db
        self.Name = name
        self.Description = description
        self.Available = is_available
        self.Enabled = is_enabled
        self.Tape = is_tape
        self.PinURL = pin_url
        self.PollURL = poll_url
        self.RemovePrefix = remove_prefix
        self.AddPrefix = add_prefix
        self.PinPrefix = pin_prefix
        self.Preference = preference
        self.Type = type                            # "dcache", "eos", etc.

    def as_dict(self):
        return dict(
            name            =   self.Name,
            description     =   self.Description,
            is_available    =   self.Available,
            is_tape         =   self.Tape,
            pin_url         =   self.PinURL,
            poll_url        =   self.PollURL,
            remove_prefix   =   self.RemovePrefix,
            add_prefix      =   self.AddPrefix,
            pin_prefix      =   self.PinPrefix,
            preference      =   self.Preference,
            is_enabled      =   self.Enabled,
            type            =   self.Type
        )

    as_jsonable = as_dict

    @classmethod
    @transactioned
    def create(cls, db, name, description="", is_enabled=False, is_available=True, is_tape=False, pin_url=None, poll_url=None, 
                remove_prefix=None, add_prefix=None, pin_prefix=None, preference=0, type=None, transaction=None):
        table = cls.Table
        columns = cls.columns(as_text=True)
        transaction.execute(f"""
                insert into {table}({columns})
                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict(name) do nothing
                """, (name, description, is_enabled, is_available, is_tape, pin_url, poll_url, remove_prefix, add_prefix, pin_prefix, preference, type)
            )
        return DBRSE.get(db, name)

    @classmethod
    def list(cls, db, include_disabled=False):
        c = db.cursor()
        table = cls.Table
        columns = cls.columns(as_text=True)
        wheres = "" if include_disabled else "where is_enabled"
        c.execute(f"""
            select {columns} from {table} {wheres}
        """)
        return (cls.from_tuple(db, tup) for tup in cursor_iterator(c))

    @transactioned
    def save(self, transaction=None):
        transaction.execute("""
            update rses 
                set description=%s, is_enabled=%s, is_available=%s, is_tape=%s, pin_url=%s, poll_url=%s, remove_prefix=%s, add_prefix=%s, pin_prefix=%s, preference=%s,
                        type=%s
                where name=%s
            """, (self.Description, self.Enabled, self.Available, self.Tape, self.PinURL, self.PollURL, self.RemovePrefix, 
                self.AddPrefix, self.PinPrefix, self.Preference, self.Type,
                self.Name)
        )


    @staticmethod
    @transactioned
    def create_many(db, names, transaction=None):
        for name in names:
            DBRSE.create(db, name, transaction=transaction)

class DBProximityMap(DBObject):

    Columns = ["cpu", "rse", "proximity"]
    PK = ["cpu", "rse"]
    Table = "proximity_map"
    
    def __init__(self, db, tuples=None, defaults = {}, overrides={}, default=None, rses=None):
        self.DB = db
        self.Defaults = defaults
        self.Overrides = overrides
        self.Default = default
        self.Map = {}                   # {cpu -> {rse -> proximity}}
        if tuples is not None:
            self._load(tuples)
        else:
            self.load()
            
        if rses is not None:
            rses = set(rses)
            for cpu, cpu_map in self.Map.items():
                for rse in list(cpu_map.keys()):        # need to convert to list because some keys may be deleted from inside the loop
                    if rse.upper() != "DEFAULT" and rse not in rses:
                        del cpu_map[rse]

    def _load(self, tuples):
        for cpu, rse, proximity in tuples:
            self.Map.setdefault(cpu, {})[rse] = proximity
    
    def load(self):
        self.Map = {}
        c = self.DB.cursor()
        c.execute(f"""
            select cpu, rse, proximity
                from {self.Table}
        """)
        self._load(cursor_iterator(c))
        return self
    
    def save(self):
        c = self.DB.cursor()
        tuples = []
        for cpu, cpu_dict in self.Map.items():
            for rse, proximity in cpu_dict.items():
                tuples.append((cpu, rse, proximity))
        try:
            c.execute("begin")
            for cpu, rse, proximity in tuples:
                c.execute(f"""
                    insert into {self.Table}(cpu, rse, proximity)
                        values(%s, %s, %s)
                        on conflict(cpu, rse)
                            do update set proximity=%s
                    """, (cpu, rse, proximity, proximity))
            c.execute("commit")
        except:
            c.execute("rollback")
            raise

    def proximity(self, cpu, rse, default="_default_"):
        if default == "_default_":
            default = self.Default
        if cpu is None: cpu = "DEFAULT"
        cpu_map = self.Map.get(cpu,  self.Map.get("DEFAULT", self.Defaults.get(cpu, {})))
        overrides = self.Overrides.get(cpu, {})
        return overrides.get(rse, cpu_map.get(rse, cpu_map.get("DEFAULT", default)))
        
    def raw(self, cpu, rse, default=None):
        return self.Map.get(cpu, {}).get(rse, default)

    def cpus(self):
        return sorted(list(self.Map.keys()), key=lambda x: "-" if x.upper() == "DEFAULT" else x)

    def rses(self):
        rses = set()
        for cpu, cpu_map in self.Map.items():
            rses |= set(rse for rse in cpu_map.keys())
        return sorted(list(rses), key=lambda x: "-" if x.upper() == "DEFAULT" else x)
