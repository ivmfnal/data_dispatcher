from webpie import HTTPServer, WPApp, WPHandler
from data_dispatcher.logs import Logged

class DataHandler(WPHandler):
    
    def add_project(self, request, relpath, project_id=None, **args):
        self.App.add_project(int(project_id))
        self.App.debug("request to add project", project_id)
        return "OK"
        
class GUIHandler(WPHandler):
    pass
    
class RootHandler(WPHandler):
    
    def __init__(self, request, app):
        self.data = DataHandler(request, app)
        self.gui = GUIHandler(request, app)

class App(WPApp, Logged):
    
    def __init__(self, project_master):
        Logged.__init__(self, "DaemonWebServerApp")
        WPApp.__init__(self, RootHandler)
        self.ProjectMaster = project_master
        
    def add_project(self, project_id):
        self.ProjectMaster.add_project(project_id)
        
class DaemonWebServer(HTTPServer):
    
    def __init__(self, config, project_master, **args):
        port = config["port"]
        app = App(project_master)
        HTTPServer.__init__(self, port, app, **args)
