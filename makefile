CC = gcc
CFLAGS = -std=c11 -O2 -Wall -fPIC
LDFLAGS = -shared

# Detect Python config
PYTHON_INCLUDE = $(shell python3-config --includes 2>/dev/null || echo "-I/usr/include/python3.8")
PYTHON_LDFLAGS = $(shell python3-config --ldflags 2>/dev/null || echo "-lpython3.8")

all: libwaldb.so

libwaldb.so: wal_db_upgraded.c hashjoin.c waldb.h
	$(CC) $(CFLAGS) $(LDFLAGS) $(PYTHON_INCLUDE) $(PYTHON_LDFLAGS) -o $@ $^

clean:
	rm -f libwaldb.so *.db *.db-wal

.PHONY: all clean