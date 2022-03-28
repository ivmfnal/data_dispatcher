PRODUCT=data_dispatcher
BUILD_DIR=$(HOME)/build/$(PRODUCT)
TARDIR=/tmp/$(USER)
LIBDIR=$(BUILD_DIR)/data_dispatcher
MODULE_DIR=$(LIBDIR)
SERVER_DIR=$(BUILD_DIR)/server
DAEMON_DIR=$(BUILD_DIR)/daemon
TARFILE=$(TARDIR)/$(PRODUCT)_$(VERSION).tar

all:	
	make VERSION=`python data_dispatcher/version.py` all_with_version
	
all_with_version: tars
	
tars:   build $(TARDIR)
	cd $(BUILD_DIR); tar cf $(TARFILE) data_dispatcher daemon server
	@echo \|
	@echo \| Tarfile is created: $(TARFILE)
	@echo \|
	

build:  clean $(BUILD_DIR) 
	cd data_dispatcher; make LIBDIR=$(LIBDIR) MODULE_DIR=$(MODULE_DIR) VERSION=$(VERSION) build
	cd web_server; make SERVER_DIR=$(SERVER_DIR) VERSION=$(VERSION)  build
	cd daemon; make DAEMON_DIR=$(DAEMON_DIR) VERSION=$(VERSION) build
	
clean:
	rm -rf $(BUILD_DIR) $(TARFILE)
	
$(TARDIR):
	mkdir -p $@
	

$(BUILD_DIR):
	mkdir -p $@
	
