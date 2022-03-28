UI_DIR=$(MODULE_DIR)/ui

FILES = api.py db.py __init__.py version.py logs.py

build:	$(MODULE_DIR) $(UI_DIR)
	cp $(FILES) $(MODULE_DIR)
	cp ui/*.py $(UI_DIR)
	
$(MODULE_DIR):
	mkdir -p $@
	
$(UI_DIR):
	mkdir -p $@
