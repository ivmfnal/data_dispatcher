UI_DIR=$(MODULE_DIR)/ui

FILES = api.py db.py __init__.py version.py

build:	$(MODULE_DIR) $(UI_DIR)
	cp $(FILES) $(MODULE_DIR)
	cd ui; make UI_DIR=$(UI_DIR) build
	cp -R logs $(MODULE_DIR)
	
$(MODULE_DIR):
	mkdir -p $@
	
$(UI_DIR):
	mkdir -p $@
