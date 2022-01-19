CC = gcc
ARCH ?= -march=core2

SRC_SRCS = $(shell ls *.c)
SRC_OBJS = $(SRC_SRCS:.c=.o)

HDR_SRCS = $(shell ls *.h)

SRCS = $(SRC_SRCS) $(TEST_SRCS)
OBJS = $(SRC_OBJS) $(TEST_OBJS)

CFLAGS = -g $(OPT) -Wall -fPIC $(INCLUDE) -std=c11 $(ARCH)
TARGET = test_riscv_table_reader
LINK = -L${COMPSTOR}/lib
LIB = -lriscv

all : OPT ?= -O2
all : MODE = all
all : $(TARGET)

debug : OPT ?= -Og
debug : CPPFLAGS+= -D DEBUG
debug : MODE = DEBUG
debug : $(TARGET)

release : OPT ?= -O3
release : CPPFLAGS+= -D NDEBUG
release : MODE = release
release : $(TARGET)

$(TARGET): ${OBJS} riscv_table_reader
	$(CC) $(CFLAGS) -o $@ ${OBJS} $(LINK) $(LIB)

.phony: riscv_table_reader
riscv_table_reader:
	${MAKE} -C ../../main/c $(MODE)

.phony: clean
clean:
	${MAKE} -C ../../main/c clean
	rm -rf $(TARGET) $(OBJS)

