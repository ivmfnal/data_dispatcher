SERVER_DIR=$(SERVER_ROOT)/server

FILES=data_server.py gui_server.py \
	projects.html project.html base.html login.html handle.html \
	rse.html rses.html proximity_map.html stats.html project_handles_log.html

build:	$(SERVER_DIR)
	cp $(FILES)  $(SERVER_DIR)
	cp -R static $(SERVER_DIR)
	
$(SERVER_DIR):
	mkdir -p $@
