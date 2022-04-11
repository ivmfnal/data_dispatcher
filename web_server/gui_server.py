from webpie import WPApp, WPHandler
from data_dispatcher.db import DBProject, DBFileHandle, DBRSE
from data_dispatcher import Version
from metacat.auth.server import AuthHandler, BaseHandler, BaseApp
import urllib, os, yaml
from urllib.parse import quote, unquote
from wsdbtools import ConnectionPool

class UsersHandler(BaseHandler):

    def users(self, request, relpath, error="", **args):
        me = self.authenticated_user()
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
        me = self.authenticated_user()
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
        me = self.authenticated_user()
        if not me.is_admin():
            self.redirect("./users?error=%s" % (quote_plus("Not authorized to create users")))
        return self.render_to_response("user.html", error=unquote_plus(error), mode="create")
        
    def save_user(self, request, relpath, **args):
        db = self.App.db()
        username = request.POST["username"]
        me = self.authenticated_user()
        
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



    def projects(self, request, relpath, message="", **args):
        db = self.App.db()
        projects = list(DBProject.list(db, with_handle_counts=True))
        for project in projects:
            ntotal = sum(project.HandleCounts.values())
            project._HandleShares = {state:float(count)/ntotal for state, count in project.HandleCounts.items()}
        if message:   message = urllib.parse.unquote_plus(message)
        return self.render_to_response("projects.html", projects=projects, handle_states = DBFileHandle.DerivedStates, message=message)

    def project(self, request, relpath, project_id=None, **args):

        state_order = {state:i for i, state in enumerate(DBFileHandle.DerivedStates)}

        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        
        if project is None:
            message = urllib.parse.quote_plus(f"Project {project_id} not found")
            self.redirect(f"./projects?message={message}")
        
        handles = sorted(project.handles(with_replicas=True), 
                key=lambda h: (state_order.get(h.State, 100), h.Attempts, h.Namespace, h.Name))
        
        handle_counts_by_state = {state:0 for state in DBFileHandle.DerivedStates}     # {state -> count}
        available_handles = 0
        for h in handles:
            replicas = h.replicas()
            h.n_replicas = len(replicas)
            h.n_available_replicas = len([r for r in replicas.values() if r.is_available()]) 
            state = h.state()
            print("handle State:", h.State, "  state():", state)
            handle_counts_by_state[state] = handle_counts_by_state.get(state, 0) + 1
            

        handle_log = list(project.handles_log())
        print("length of handle_log:", len(handle_log))

        return self.render_to_response("project.html", project=project, 
                    handles=handles,
                    available_handles=available_handles,
                    handle_counts_by_state=handle_counts_by_state, states=DBFileHandle.DerivedStates,
                    handle_log = handle_log
        )

    def handle(self, request, relpath, project_id=None, namespace=None, name=None, **args):
        db = self.App.db()
        handle = DBFileHandle.get(db, int(project_id), namespace, name)
        if handle is None:
            self.redirect(f"./project?project_id={project_id}&error=Handle+not+found")
        return self.render_to_response("handle.html", project_id=project_id, handle=handle)


class RSEHandler(BaseHandler):
    
    def rses(self, request, relpath, **args):
        user = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        rses = list(DBRSE.list(self.App.db()))
        rses = sorted(rses, key=lambda r: r.Name)
        return self.render_to_response("rses.html", rses=rses, is_admin=is_admin)
    
    index = rses
    
    def rse(self, request, relpath, name=None, **args):
        name = name or relpath
        rse = DBRSE.get(self.App.db(), name)
        if rse is None:
            self.redirect("./rses")
        user = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        mode = "edit" if is_admin else "view"
        return self.render_to_response("rse.html", rse=rse, mode=mode)
    
    def create(self, request, relpath, **args):
        user = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        if not is_admin:
            self.redirect("./rses")
        return self.render_to_response("rse.html", is_admin=is_admin, mode="create")
        
    def do_create(self, request, relpath, **args):
        user = self.authenticated_user()
        is_admin = user is not None and user.is_admin()
        if not is_admin:
            self.redirect("./rses")
        
        name = request.POST["name"]
        rse = DBRSE.create(self.App.db(), name)
        self._do_update(rse, request)
        self.redirect(f"./rses")
        
    def do_update(self, request, relpath, **args):
        user = self.authenticated_user()
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
        rse.Description = request.POST.get("description", "")
        rse.Preference = int(request.POST.get("preference", 0))
        rse.save()


class TopHandler(BaseHandler):
    
    def __init__(self, request, app):
        BaseHandler.__init__(self, request, app)
        self.U = UsersHandler(request, app)
        self.P = ProjectsHandler(request, app)
        self.A = AuthHandler(request, app)
        self.R = RSEHandler(request, app)
        
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


class App(BaseApp):
    
    def __init__(self, config):
        BaseApp.__init__(self, config, TopHandler)
        self.Config = config
        
    def init(self):
        templdir = self.ScriptHome
        self.initJinjaEnvironment(
            tempdirs=[templdir, "."],
            globals={
                "GLOBAL_Version": Version, 
                "GLOBAL_SiteTitle": self.Config.get("site_title", "DEMO Data Dispatcher")
            },
            filters = {
                "pretty_time_delta": pretty_time_delta
            }
        )

def create_application(config):
    if isinstance(config, str):
        config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)
    return App(config)
    
        
if __name__ == "__main__":
    import getopt, sys
    
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    server_config = config.get("server", {})
    port = server_config.get("gui_port", 8080)
    app = create_application(config)
    app.run_server(port, logging=True)
    
