ifeq ($(RVV),0)
	CC = gcc
	CXX= g++
else ifeq ($(RVV),1) # scalar implementation compiled with riscv
	CC = $(RISCV)/bin/clang
	CXX= $(RISCV)/bin/clang++
	ARCH = --gcc-toolchain=$(RISCV) --target=riscv64 -menable-experimental-extensions -march=rv64imv0p10 -mabi=lp64
else ifeq ($(RVV),2)
	CC = riscv64-unknown-elf-gcc
	CXX= riscv64-unknown-elf-g++
	CPPFLAGS += -D RVV
	ARCH = -march=rv64imv -mabi=lp64
else
	CC = $(RISCV)/bin/clang
	CXX= $(RISCV)/bin/clang++
	CPPFLAGS += -D RVV
	ARCH = --gcc-toolchain=$(RISCV) --target=riscv64 -menable-experimental-extensions -march=rv64imv0p10 -mabi=lp64
endif

SRC_SRCS = $(shell ls ../*.c) $(shell ls *.c)
SRC_OBJS = $(SRC_SRCS:.c=.o)

HDR_SRCS = $(shell ls ../*.h)

SRCS = $(SRC_SRCS) $(TEST_SRCS)
OBJS = $(SRC_OBJS) $(TEST_OBJS)

INCLUDE = -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I..

CFLAGS = -static -g $(OPT) -Wall -fPIC $(INCLUDE) -std=c11 $(ARCH)

TARGET = riscv_table_reader

all : OPT ?= -O2
all : ARCH ?= -march=core2
all : ${TARGET}

debug : OPT = -Og
debug : ARCH ?= -march=core2
debug : CFLAGS+= -D DEBUG
debug : ${TARGET}

release : OPT = -O3
release : ARCH ?= -march=core2
release : CFLAGS += -D NDEBUG
release : ${TARGET}

$(TARGET): ${OBJS}
	$(CC) $(CFLAGS) -o $@ $^ $(LINK) $(LIB)

.phony: clean
clean:
	${MAKE} -C .. clean
	rm -rf $(TARGET) $(OBJS)
