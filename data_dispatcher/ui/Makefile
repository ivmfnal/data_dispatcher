BINFILES = dd
LIBFILES = __init__.py ui.py ui_project.py ui_handle.py ui_lib.py

build:	$(UI_DIR)
	cp $(BINFILES) $(LIBFILES) $(UI_DIR)
	cd $(UI_DIR); chmod +x $(BINFILES)

$(UI_DIR):
	mkdir -p $@
