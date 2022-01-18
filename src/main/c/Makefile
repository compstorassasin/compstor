SRC_SRCS = $(shell ls *.c)
SRC_OBJS = $(SRC_SRCS:.c=.o)

HDR_SRCS = $(shell ls *.h)

SRCS = $(SRC_SRCS) $(TEST_SRCS)
OBJS = $(SRC_OBJS) $(TEST_OBJS)

INCLUDE = -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux

CFLAGS = -g -Wall -fPIC $(INCLUDE) -march=native -std=c11
TARGET = ../../../lib/libriscv.so

default : OPT = -O2
default : $(TARGET)

release : OPT = -O3
release : CFLAGS += -D NDEBUG
release : $(TARGET)

debug : OPT = -O0
debug : CFLAGS+= -D DEBUG
debug : $(TARGET)

CFLAGS += $(OPT)

$(TARGET): ${OBJS}
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LINK) $(LIB)

.phony: clean
clean:
	rm -rf $(TARGET) $(OBJS)
