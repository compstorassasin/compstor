SRC_SRCS = test_riscv_table_reader.c
SRC_OBJS = $(SRC_SRCS:.c=.o)

HDR_SRCS = $(shell ls *.h)

SRCS = $(SRC_SRCS) $(TEST_SRCS)
OBJS = $(SRC_OBJS) $(TEST_OBJS)

OPT ?= -O3

CFLAGS = -g $(OPT) -Wall -fPIC $(INCLUDE) -std=c11
CXXFLAGS = -g $(OPT) -Wall -fPIC $(INCLUDE) -std=c++14
TARGET = test_riscv_table_reader
LINK = -L${COMPSTOR}/lib
LIB = -lriscv

ifdef DEBUG
	OPT=-O0
	CFLAGS+= -D DEBUG
	CXXFLAGS+= -D DEBUG
else
	CFLAGS+= -D NDEBUG
	CXXFLAGS+= -D NDEBUG
endif

ifdef RISCV32
	CC = riscv64-unknown-elf-gcc
	CXX = riscv64-unknown-elf-g++
	AR = riscv64-unknown-elf-ar
	AS = riscv64-unknown-elf-as

	CFLAGS += -march=rv32i -mabi=ilp32
	CPPFLAGS += -march=rv32i -mabi=ilp32
else
	CFLAGS += -march=native
	CPPFLAGS += -march=native
endif

$(TARGET): ${OBJS} riscv_table_reader
	$(CC) $(CFLAGS) -o $@ ${OBJS} $(LINK) $(LIB)

.phony: riscv_table_reader
riscv_table_reader:
	${MAKE} -C ../../main/c/

.phony: clean
clean:
	${MAKE} -C ../../main/c/ clean
	rm -rf $(TARGET) $(OBJS)

