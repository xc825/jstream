CC = gcc

CFLAGS = -O3 -Wall -Wextra -Werror -std=c99 -pedantic

LIBDIRS = -L/usr/local/apr/lib -L/usr/lib/x86_64-linux-gnu

LIBS = -lapr-1

INC = -I/usr/local/apr/include/apr-1

DEFS = -DPATH_MAX=4096

all: jstream

jstream:
	$(CC) -o jstream jstream.c cJSON.c  $(DEFS) $(CFLAGS) $(INC) $(LIBDIRS) $(LIBS)

clean:	
	rm -f jstream
