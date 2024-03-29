from webpie import WPApp, WPHandler, WPStaticHandler
from data_dispatcher.db import DBProject, DBFileHandle, DBRSE, DBProximityMap       # , DBUser
from data_dispatcher import Version
from metacat.auth.server import AuthHandler, BaseHandler, BaseApp
import urllib, os, yaml, time
from urllib.parse import quote, unquote, unquote_plus
from wsdbtools import ConnectionPool
from datetime import timezone

class ___UsersHandler(BaseHandler):

    def users(self, request, relpath, error="", **args):
        me, auth_error = self.authenticated_user()
        if not me:
            self.redirect("../A/login?redirect=" + self.scriptUri() + "/U/users")
        db = self.App.db()
        users = sorted(list(DBUser.list(db)), key=lambda u: u.Username)
        #print("Server.users: users:", users)
        
        index = None
        if len(users) > 30:
            alphabet = set(u.Username[0] for u in users)
            index = {}
            for u in users:
                a = u.Username[0]
                if not a in index:
                    index[a] = u.Username

        return self.render_to_response("users.html", users=users, error=unquote_plus(error), admin = me.is_admin(),
                index = index)
        
    def user(self, request, relpath, username=None, error="", message="", **args):
        db = self.App.connect()
        user = DBUser.get(db, username)
        me, auth_error = self.authenticated_user()
        ldap_config = self.App.auth_config("ldap")
        ldap_url = ldap_config and ldap_config["server_url"]
        return self.render_to_response("user.html", user=user, 
            ldap_url = ldap_url, 
            error = unquote_plus(error), message=unquote_plus(message),
            mode = "edit" if (me.is_admin() or me.Username==username) else "view", 
            its_me = me.Username==username,
            admin=me.is_admin())
            
    def create_user(self, request, relpath, error="", **args):
        db = self.App.db()
        me, auth_error = self.authenticated_user()
        if not me.is_admin():
            self.redirect("./users?error=%s" % (quote_plus("Not authorized to create users")))
        return self.render_to_response("user.html", error=unquote_plus(error), mode="create")
        
    def save_user(self, request, relpath, **args):
        db = self.App.db()
        username = request.POST["username"]
        me, auth_error = self.authenticated_user()
        
        new_user = request.POST["new_user"] == "yes"
        
        u = DBUser.get(db, username)
        if u is None:   
            if not new_user:    
                self.redirect("./users?error=%s", quote_plus("user not found"))
            u = DBUser(db, username, request.POST["name"], request.POST["email"], request.POST["flags"])
        else:
            u.Name = request.POST["name"]
            u.EMail = request.POST["email"]
            if me.is_admin():   u.Flags = request.POST["flags"]
            
        if me.is_admin() or me.Username == u.Username:

            if "save_user" in request.POST:
                password = request.POST.get("password1")
                if password:
                    u.set_auth_info("password", None, password)
                
                if me.is_admin():
                    u.set_auth_info("ldap", self.App.auth_config("ldap"), "allow_ldap" in request.POST)
                
                u.save()
            elif "add_dn" in request.POST:
                dn = request.POST.get("new_dn")
                if dn:
                    dn_list = u.authenticator("x509").Info or []
                    if not dn in dn_list:
                        dn_list.append(dn)
                        u.set_auth_info("x509", None, dn_list)
                        u.save()
            else:
                for k, v in request.POST.items():
                    if k.startswith("remove_dn:"):
                        dn = k.split(":",1)[-1]
                        break
                else:
                    dn = None
                if dn:
                    dn_list = u.authenticator("x509").Info or []
                    while dn in dn_list:
                        dn_list.remove(dn)
                    u.set_auth_info("x509", None, dn_list)
                    u.save()
                    
class ProjectsHandler(BaseHandler):
    
    def projects(self, request, relpath, message="", page=0, page_size=100, **args):
        db = self.App.db()
        page = int(page)
        page_size = int(page_size)
        istart = page*page_size
        projects = list(DBProject.list(db, with_handle_counts=True))
        nprojects = len(projects)
        npages = (nprojects + page_size - 1)//page_size
        projects = projects[istart:istart + page_size]
        for project in projects:
            ntotal = sum(project.HandleCounts.values())
            project._HandleShares = {state:float(count)/ntotal for state, count in project.HandleCounts.items()}
        if message:   message = urllib.parse.unquote_plus(message)
        
        next_page = page + 1
        prev_page = page - 1
        next_page_link = f"projects/page={next_page}&page_size={page_size}"
        
        return self.render_to_response("projects.html", projects=projects, handle_states = DBFileHandle.DerivedStates, message=message)

    def project(self, request, relpath, project_id=None, page=0, page_size=100, **args):

        page = int(page)
        page_size = int(page_size)
        state_order = {state:i for i, state in enumerate(DBFileHandle.DerivedStates)}

        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        
        if project is None:
            message = urllib.parse.quote_plus(f"Project {project_id} not found")
            self.redirect(f"./projects?message={message}")

        handles = sorted(project.handles(with_replicas=False), 
                key=lambda h: (state_order.get(h.State, 100), h.Attempts, h.Namespace, h.Name))

        nhandles = len(handles)
        istart = page*page_size
        t0 = time.time()
        handles = list(project.get_handles([h.did() for h in handles[istart:istart+page_size]]))
        print(time.time() - t0)
        npages = (nhandles + page_size - 1)//page_size
        last_page = npages - 1
        next_page = page + 1
        prev_page = page - 1
        next_page_link = f"project?project_id={project_id}&page={next_page}&page_size={page_size}" if next_page < npages else None
        prev_page_link = f"project?project_id={project_id}&page={prev_page}&page_size={page_size}" if prev_page >= 0 else None
        first_page_link = f"project?project_id={project_id}&page=0&page_size={page_size}" if npages > 1 else None
        last_page_link = f"project?project_id={project_id}&page={last_page}&page_size={page_size}" if npages > 1 else None

        handle_counts_by_state = {state:0 for state in DBFileHandle.DerivedStates}     # {state -> count}
        available_handles = 0
        for h in handles:
            replicas = h.replicas()
            h.n_replicas = len(replicas)
            h.n_available_replicas = len([r for r in replicas.values() if r.is_available()]) 
            state = h.state()
            #print("handle State:", h.State, "  state():", state)
            handle_counts_by_state[state] = handle_counts_by_state.get(state, 0) + 1
            
        handles_log = {}            # {did -> [log record, ...]}
        files_log = {}              # {did -> [log record, ...]}
        combined_log = {}
        project_log = project.get_log()
        
        for log_record in project.handles_log():
            #print("gui.project(): handle log_record:", log_record)
            did = log_record.Namespace + ":" + log_record.Name
            handles_log.setdefault(did, []).append(log_record)
            combined_log.setdefault(did, []).append(log_record)

        if False:
            for log_record in project.files_log():
                did = log_record.Namespace + ":" + log_record.Name
                files_log.setdefault(did, []).append(log_record)
                combined_log.setdefault(did, []).append(log_record)

            for did in list(combined_log.keys()):
                combined_log[did] = sorted(combined_log[did], key=lambda r: r.T)
        #print("gui.project(): handles_log:", handles_log)
    
        return self.render_to_response("project.html", project=project,
            handles=handles,
            available_handles=available_handles,
            handle_counts_by_state=handle_counts_by_state, states=DBFileHandle.DerivedStates,
            project_log = project.get_log(),
            files_log = files_log,
            handles_log = handles_log,
            combined_log = combined_log,
            page = page, prev_page = prev_page, next_page = next_page, last_page = last_page,
            next_page_link = next_page_link, prev_page_link = prev_page_link,
            last_page_link = last_page_link, first_page_link= first_page_link
        )

    def handle(self, request, relpath, project_id=None, namespace=None, name=None, **args):
        db = self.App.db()
        handle = DBFileHandle.get(db, int(project_id), namespace, name)
        if handle is None:
            self.redirect(f"./project?project_id={project_id}&error=Handle+not+found")
        return self.render_to_response("handle.html", project_id=project_id, handle=handle, handle_log = list(handle.get_log(reversed=True)))

class RSEHandler(BaseHandler):
    
    def proximity_map(self, request, relpath, message="", **args):
        enabled_rses = set(r.Name for r in DBRSE.list(self.App.db(), include_disabled=False))
        pmap = self.App.proximity_map(rses=enabled_rses)
        overrides = pmap.Overrides
        overrides_cpus = sorted(overrides.keys(), key=lambda x: "-" if x.upper() == "DEFAULT" else x)
        overrides_rses = set()
        for m in overrides.values():
            overrides_rses |= set(m.keys())
        overrides_rses = sorted(overrides_rses, key=lambda x: "-" if x.upper() == "DEFAULT" else x)
        return self.render_to_response("proximity_map.html", proximity_map = pmap, default_proximity=pmap.Default,
            overrides = overrides, overrides_cpus=overrides_cpus, overrides_rses=overrides_rses
        )

    def rses(self, request, relpath, **args):
        user, auth_error = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        rses = list(DBRSE.list(self.App.db(), include_disabled=True))
        rses = sorted(rses, key=lambda r: (0 if r.Enabled else 1, r.Name))
        return self.render_to_response("rses.html", rses=rses, is_admin=is_admin)
    
    index = rses
    
    def rse(self, request, relpath, name=None, **args):
        name = name or relpath
        rse = DBRSE.get(self.App.db(), name)
        if rse is None:
            self.redirect("./rses")
        user, auth_error = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        mode = "edit" if is_admin else "view"
        return self.render_to_response("rse.html", rse=rse, mode=mode)

    def create(self, request, relpath, **args):
        user, auth_error = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        if not is_admin:
            self.redirect("./rses")
        return self.render_to_response("rse.html", is_admin=is_admin, mode="create")

    def parse_proximity_map(self, text):
        pmap = []
        for line in text.split("\n"):
            line = line.strip()
            if line:
                site, proximity = line.split(":", 1)
                site = site.strip()
                proximity = int(proximity.strip())
                pmap.append([site, proximity])

    def do_create(self, request, relpath, **args):
        user, auth_error = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        if not is_admin:
            self.redirect("./rses")
        
        name = request.POST["name"]
        rse = DBRSE.create(self.App.db(), name)
        self._do_update(rse, request)
        self.redirect(f"./rses")
        
    def do_update(self, request, relpath, **args):
        user, auth_error = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        if not is_admin:
            self.redirect("./rses")
        
        name = request.POST["name"]
        rse = DBRSE.get(self.App.db(), name)
        if rse is None:
            self.redirect("./rses")
        self._do_update(rse, request)
        self.redirect(f"./rses")
        
    def _do_update(self, rse, request):
        rse.Tape = request.POST.get("is_tape", "no") != "no"
        rse.Available = request.POST.get("is_available", "no") != "no"
        rse.RemovePrefix = request.POST.get("remove_pefix", "")
        rse.AddPrefix = request.POST.get("add_pefix", "")
        rse.PollURL = request.POST.get("poll_url") or None
        rse.PinURL = request.POST.get("pin_url") or None
        rse.PinPrefix = request.POST.get("pin_prefix") or None
        rse.Description = request.POST.get("description", "")
        rse.Preference = int(request.POST.get("preference", 0))
        rse.Enabled = request.POST.get("is_enabled", "no") != "no"
        rse.save()


class TopHandler(BaseHandler):
    
    def __init__(self, request, app):
        BaseHandler.__init__(self, request, app)
        #self.U = UsersHandler(request, app)
        self.P = ProjectsHandler(request, app)
        self.A = AuthHandler(request, app)
        self.R = RSEHandler(request, app)
        self.static = WPStaticHandler(request, app)
        
    def index(self, request, relpath, **args):
        self.redirect("P/projects")


def pretty_time_delta(t):
    if t == None:   return ""
    sign = ''
    if t < 0:   
        sign = '-'
        t = -t
    seconds = t
    if seconds < 60:
        out = '%.1fs' % (seconds,)
    elif seconds < 3600:
        seconds = int(seconds)
        minutes = seconds // 60
        seconds = seconds % 60
        out = '%dm%ds' % (minutes, seconds)
    elif seconds < 3600*24:
        seconds = int(seconds)
        minutes = seconds // 60
        hours = minutes // 60
        minutes = minutes % 60
        out = '%dh%02dm' % (hours, minutes)
    else:
        seconds = int(seconds)
        minutes = seconds // 60
        hours = minutes // 60
        minutes = minutes % 60
        days = hours // 24
        hours = hours % 24
        out = "%dd%02dh%02dm" % (days, hours, minutes)
        
    return sign + out

def as_dt_utc(t):
    from datetime import datetime
    if t is None:   return ""
    if isinstance(t, datetime):
        t = t.timestamp()
    if isinstance(t, (int, float)):
        t = datetime.fromtimestamp(t, timezone.utc)
    return t.strftime("%D&nbsp;%H:%M:%S")
    
def pprint_data(data):
    import pprint
    return pprint.pformat(data, indent=2)
    
def format_log_data(data):
    import pprint
    parts = []
    need_break = False
    first_line = True
    for k, v in sorted(data.items()):
        formatted = '%s=<span style="white-space:pre">%s</span>&nbsp; ' % (k, pprint.pformat(v))
        if not first_line and (isinstance(v, (dict, list)) or need_break):
            parts.append("<br/>")
        parts.append(formatted)
        need_break = isinstance(v, (dict, list))
        first_line = False
    return "".join(parts)
    
def none_as_blank(x):
    if x is None:   return ''
    else: return str(x)

class App(BaseApp):
    
    def __init__(self, config, prefix):
        BaseApp.__init__(self, config, TopHandler, prefix=prefix)
        self.Config = config
        proximity_cfg = config.get("proximity_map", {})
        self.DefaultProximity = proximity_cfg.get("default_proximity", -1)
        self.PMDefaults = proximity_cfg.get("defaults", {})
        self.PMOverrides = proximity_cfg.get("overrides", {})
        self.SiteTitle = config.get("web_server", {}).get("site_title", "DEMO Data Dispatcher")
        self.MetaCatURL = config.get("metacat_url")

    def proximity_map(self, rses=None):
        return DBProximityMap(self.db(), default=self.DefaultProximity, rses=rses, defaults=self.PMDefaults, overrides=self.PMOverrides)

    def init(self):
        #print("App.init... prefix:", self.Prefix)
        templdir = self.ScriptHome
        self.initJinjaEnvironment(
            tempdirs=[templdir, "."],
            globals={
                "GLOBAL_Version": Version, 
                "GLOBAL_SiteTitle": self.SiteTitle,
                "GLOBAL_MetaCatURL": self.MetaCatURL
            },
            filters = {
                "pretty_time_delta": pretty_time_delta,
                "format_log_data": format_log_data,
                "as_dt_utc": as_dt_utc,
                "none_as_blank": none_as_blank
            }
        )

def create_application(config):
    if isinstance(config, str):
        config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)
    server_config = config.get("web_server", {})
    prefix = server_config.get("gui_prefix", "")
    return App(config, prefix)
    
        
if __name__ == "__main__":
    import getopt, sys
    
    opts, args = getopt.getopt(sys.argv[1:], "c:l")
    opts = dict(opts)
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    server_config = config.get("web_server", {})
    port = server_config.get("gui_port", 8080)
    print("Starting on port:", port)
    logging = "-l" in opts or server_config.get("gui_logging")
    app = create_application(config)
    app.run_server(port, logging=logging)
    
