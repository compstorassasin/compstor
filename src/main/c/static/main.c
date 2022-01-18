#include <stdio.h>
#include <stdlib.h>

#include "riscv_jni.h"

static const int buffer_bytes = 256 << 20;

int main(int argc, char* argv[]) {
  char json[4096];
  FILE* ji = fopen(argv[2], "rb");
  int bytes = fread(json, 1, 4096, ji);
  json[bytes] = 0;
  char* output = (char*)malloc(buffer_bytes);

  void* reader = riscv_create_reader(argv[1], json);

  return riscv_next_batch(reader, output);
}