BINFILES = ddisp
UI_LIBDIR = $(CLIENT_LIBDIR)/ui
LIBFILES = ui_main.py ui_project.py ui_file.py ui_lib.py ui_rse.py ui_worker.py 

build:	$(UI_LIBDIR) $(CLIENT_BINDIR)
	cp $(LIBFILES) $(UI_LIBDIR)
	mkdir $(UI_LIBDIR)/cli
	cp cli/*.py $(UI_LIBDIR)/cli
	cp $(BINFILES) $(CLIENT_BINDIR)
	cd $(CLIENT_BINDIR); chmod +x $(BINFILES)

$(UI_LIBDIR):
	mkdir -p $@

$(CLIENT_BINDIR):
	mkdir -p $@
