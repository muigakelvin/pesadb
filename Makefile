CC = gcc
CFLAGS = -std=c11 -O2 -Wall -fPIC
LDFLAGS = -shared

PYTHON_INCLUDE := $(shell python3-config --includes 2>/dev/null)
PYTHON_LDFLAGS := $(shell python3-config --ldflags 2>/dev/null)

ifeq ($(PYTHON_INCLUDE),)
$(error "Python headers not found. Install python3-dev or python3-devel")
endif

# Source and build directories
SRC_DIR := src/c
BUILD_DIR := build
DATA_DIR := data

# Explicitly list only the correct source files
C_SRCS := $(SRC_DIR)/wal_db_upgraded.c $(SRC_DIR)/hashjoin.c
OBJ_TARGET := $(BUILD_DIR)/libwaldb.so

all: $(OBJ_TARGET)

# Ensure build directory exists
$(BUILD_DIR):
	@mkdir -p $@

# Build the shared library
$(OBJ_TARGET): $(C_SRCS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) $(PYTHON_INCLUDE) $(PYTHON_LDFLAGS) \
		-I$(SRC_DIR) -o $@ $^

# Clean build artifacts and database files
clean:
	rm -rf $(BUILD_DIR)
	rm -f $(DATA_DIR)/*.pesa $(DATA_DIR)/*.pesa-wal

.PHONY: all clean