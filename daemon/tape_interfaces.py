from wlcg import DCacheInterface
from pythreader import Primitive, synchronized

class InterfaceLibrary(Primitive):
    
    def __init__(self):
        Primitive.__init__(self)
        self.Interfaces = {}
    
    @synchronized
    def get_interface(self, rse, rse_config, db):
        interface = self.Interfaces.get(rse)
        type = rse_config.type(rse)
        if interface is None:
            try:    interface_class = {
                    "dcache":   DCacheInterface
                }[type]
            except KeyError:
                raise ValueError(f"Unknown tape RSE type: {type}")
            interface = interface_class(rse, db, rse_config)
            self.Interfaces[rse] = interface
        return interface

    def interfaces(self):
        return self.Interfaces

_Library = InterfaceLibrary()

def get_interface(rse, rse_config, db):
    return _Library.get_interface(rse, rse_config, db)
    
def interfaces():
    return _Library.interfaces()