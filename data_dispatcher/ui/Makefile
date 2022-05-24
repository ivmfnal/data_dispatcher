BINFILES = dd
LIBFILES = __init__.py ui.py ui_project.py ui_file.py ui_lib.py ui_rse.py ui_worker.py 

build:	$(UI_DIR)
	cp $(BINFILES) $(LIBFILES) $(UI_DIR)
	mkdir $(UI_DIR)/cli
	cp cli/*.py $(UI_DIR)/cli
	cd $(UI_DIR); chmod -R +x $(BINFILES) cli

$(UI_DIR):
	mkdir -p $@
