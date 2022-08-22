PRODUCT=data_dispatcher
BUILD_DIR=$(HOME)/build/$(PRODUCT)
TARDIR=/tmp/$(USER)
LIBDIR=$(BUILD_DIR)/data_dispatcher
BINDIR=$(BUILD_DIR)/bin
MODULE_DIR=$(LIBDIR)
SERVER_ROOT=$(BUILD_DIR)/server
CLIENT_ROOT=$(BUILD_DIR)/client
DAEMON_DIR=$(SERVER_ROOT)/daemon
SERVER_TAR=$(TARDIR)/$(PRODUCT)_$(VERSION).tar
CLIENT_TAR=$(TARDIR)/$(PRODUCT)_client_$(VERSION).tar

all:
	make VERSION=`python data_dispatcher/version.py` all_with_version
	
all_with_version: tars
	
tars:   build $(TARDIR)
	cd $(SERVER_ROOT); tar cf $(SERVER_TAR) data_dispatcher daemon server
	cd $(CLIENT_ROOT); tar cf $(CLIENT_TAR) data_dispatcher bin canned_client_setup.sh
	@echo \|
	@echo \| "Tarfiles created:"
	@echo \| "    Server and daemon:" $(SERVER_TAR)
	@echo \| "    Client:           " $(CLIENT_TAR)
	@echo \|

build: clean $(BUILD_DIR)
	cd data_dispatcher; make SERVER_ROOT=$(SERVER_ROOT) CLIENT_ROOT=$(CLIENT_ROOT) VERSION=$(VERSION) build
	cd web_server; make SERVER_ROOT=$(SERVER_ROOT) VERSION=$(VERSION)  build
	cd daemon; make DAEMON_DIR=$(DAEMON_DIR) VERSION=$(VERSION) build
	cp canned_client_setup.sh $(CLIENT_ROOT)
	find $(BUILD_DIR) -type d -name __pycache__ -print | xargs rm -rf
	find $(BUILD_DIR) -type f -name \*.pyc -print -exec rm {} \;
	
clean:
	rm -rf $(BUILD_DIR) $(TARFILE)
	
$(TARDIR):
	mkdir -p $@
	
$(BUILD_DIR):
	mkdir -p $@
