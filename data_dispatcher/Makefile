CLIENT_BINDIR=$(CLIENT_ROOT)/bin
CLIENT_LIBDIR=$(CLIENT_ROOT)/data_dispatcher
SERVER_LIBDIR=$(SERVER_ROOT)/data_dispatcher

SERVER_FILES = api.py db.py __init__.py version.py
CLIENT_FILES = api.py __init__.py version.py

build:	server client
	
server:	$(SERVER_LIBDIR)
	cp $(SERVER_FILES) $(SERVER_LIBDIR)
	cp -R logs $(SERVER_LIBDIR)

client: $(CLIENT_BINDIR) $(CLIENT_LIBDIR)
	cp $(CLIENT_FILES) $(CLIENT_LIBDIR)
	cd ui; make CLIENT_LIBDIR=$(CLIENT_LIBDIR) CLIENT_BINDIR=$(CLIENT_BINDIR) build
	
$(CLIENT_BINDIR):
	mkdir -p $@

$(CLIENT_LIBDIR):
	mkdir -p $@

$(SERVER_LIBDIR):
	mkdir -p $@
