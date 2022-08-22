BINFILES = ddisp
LIBFILES = ui_main.py ui_project.py ui_file.py ui_lib.py ui_rse.py ui_worker.py 

build:	$(CLIENT_LIBDIR) $(CLIENT_BINDIR)
	cp $(LIBFILES) $(CLIENT_LIBDIR)
	mkdir $(CLIENT_LIBDIR)/cli
	cp cli/*.py $(CLIENT_LIBDIR)/cli
	cp $(BINFILES) $(CLIENT_BINDIR)
	cd $(CLIENT_BINDIR); chmod +x $(BINFILES)

$(CLIENT_LIBDIR):
	mkdir -p $@

$(CLIENT_BINDIR):
	mkdir -p $@
