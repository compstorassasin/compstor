#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "riscv_table_reader.h"

static const int buffer_bytes = 256 << 20;

int main(int argc, char* argv[]) {
  assert(argc == 5);

  const int json_bytes = 4096;

  char* scratchpad = NULL;
  char* streambuffer = NULL;
  scratchpad = (char*)(atol(argv[3]));
  streambuffer = (char*)(atol(argv[4]));

  char* json = NULL;
  riscv_table_reader* reader = NULL;
  char* input_buffer = NULL;
  char* output_buffer = NULL;

  if (scratchpad) {
    json = scratchpad;
    reader = (riscv_table_reader *)(scratchpad + json_bytes);
  } else {
    json = (char*)malloc(json_bytes);
  }
  if (streambuffer) {
    input_buffer = streambuffer;
    output_buffer = streambuffer + buffer_bytes;
  } else {
    output_buffer = (char*)malloc(buffer_bytes);
  }

  FILE* ji = fopen(argv[2], "rb");
  int bytes = fread(json, 1, json_bytes, ji);
  assert(bytes < json_bytes - 1);
  json[bytes] = 0;

  reader = riscv_table_reader_init(argv[1], json, reader, input_buffer);
  int rows = riscv_table_reader_next_batch(reader, output_buffer);
  printf("orows %d\n", rows);

  if (!scratchpad) free(json);
  if (!streambuffer) free(output_buffer);
}