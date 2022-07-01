import traceback, sys, time
from pythreader import LogFile, LogStream

class Logger(object):

    def __init__(self, log_output, debug_enabled=False, debug_out=None, error_out=None):
        self.LogOut = self.make_log(log_output, sys.stdout)
        self.ErrorOut = self.make_log(error_out, sys.stderr)
        self.DebugOut = None if not debug_enabled else self.make_log(debug_out, sys.stdout)
        self.DebugEnabled = debug_enabled
        
    def make_log(self, output, dash_stream, **params):
        if output is None:  
            return None
        elif isinstance(output, (LogFile, LogStream)):
            return output
        elif output == "-":
            return LogStream(dash_stream)
        elif output is sys.stderr or output is sys.stdout:
            return LogStream(output)
        else:
            out = LogFile(output, **params)
            out.start()
            return out
            
    def log(self, who, *parts, sep=" "):
        if self.LogOut is not None:
            self.LogOut.log("%s: %s" % (who, sep.join([str(p) for p in parts])))
            
    def debug(self, who, *parts, sep=" "):
        if self.DebugEnabled and self.DebugOut is not None:
            self.DebugOut.log("%s [DEBUG]: %s" % (who, sep.join([str(p) for p in parts])))

    def error(self, who, *parts, sep=" "):
        if self.ErrorOut is not None:
            self.ErrorOut.log("%s [ERROR]: %s" % (who, sep.join([str(p) for p in parts])))
            
class Logged(object):

    def __init__(self, name, logger, debug=True):
        self.LogName = name
        self.Logger = logger
        self.Debug = debug
        
    def debug(self, *params):
        if self.Debug:
            self.Logger.debug(self.LogName, *params)

    def log(self, *params):
        self.Logger.log(self.LogName, *params)

    def error(self, *params):
        self.Logger.error(self.LogName, *params)

