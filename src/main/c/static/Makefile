SRC_SRCS = $(shell ls ../*.c) $(shell ls *.c)
SRC_OBJS = $(SRC_SRCS:.c=.o)

HDR_SRCS = $(shell ls ../*.h)

SRCS = $(SRC_SRCS) $(TEST_SRCS)
OBJS = $(SRC_OBJS) $(TEST_OBJS)

INCLUDE = -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I..

CFLAGS = -static -g -Wall -fPIC $(INCLUDE) -std=c11

TARGET = riscv_table_reader

default : CC=${RISCV}/bin/riscv64-unknown-elf-gcc
default : CFLAGS+= -march=rv64im -mabi=lp64
default : OPT = -O2
default : ${TARGET}

release : OPT = -O3
release : CFLAGS += -D NDEBUG
release : ${TARGET}

debug : OPT = -O0
debug : CFLAGS+= -D DEBUG
debug : ${TARGET}

riscv : CC=${RISCV}/bin/clang
riscv : CFLAGS+=--gcc-toolchain=${RISCV} --target=riscv64 -march=rv64im -mabi=lp64
riscv : $(TARGET)

CFLAGS += $(OPT)

$(TARGET): ${OBJS}
	$(CC) $(CFLAGS) -o $@ $^ $(LINK) $(LIB)

.phony: clean
clean:
	${MAKE} -C .. clean
	rm -rf $(TARGET) $(OBJS)
