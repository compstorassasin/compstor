#include <assert.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../main/c/riscv_jni.h"

static const int buffer_bytes = 256 << 20;

int check_output(char* output, int rows, int min_row_bytes, int max_row_bytes) {
  printf("Processing %d rows...\n", rows);
  int bytes = 0;
  for (char* out = output; rows > 0; rows--) {
    int* row = (int*)out;
    assert(0 == row[0]);
    assert(row[1] >= min_row_bytes);
    if (max_row_bytes) {
      assert(row[1] <= max_row_bytes);
    }
    bytes += (row[1] + 8);
    out += (row[1] + 8);
  }
  return bytes;
}

void Q56_lineitem() {
  printf("Q56_lineitem ...\n");
  char path[256];
  strcpy(path, getenv("COMPSTOR"));
  int len = strlen(path);
  strcpy(path + len, "/tpch/tbl_s1e1/lineitem/00001.tbl");

  char json[4096];
  FILE* ji = fopen("Q56_lineitem.json", "rb");
  int bytes = fread(json, 1, 4096, ji);
  assert(bytes < 4095);
  json[bytes] = 0;
  void* reader = riscv_create_reader(path, json);

  char* output = (char*)malloc(buffer_bytes);
  int rows = riscv_next_batch(reader, output);
  bytes = check_output(output, rows, 16, 0);
  printf("rows: %d, bytes: %d\n", rows, bytes);
  printf("\n");
}

void Q61_nation() {
  printf("Q61_nation ...\n");
  char path[256];
  strcpy(path, getenv("COMPSTOR"));
  int len = strlen(path);
  strcpy(path + len, "/tpch/tbl_s1e1/nation/00000.tbl");

  char json[4096];
  FILE* ji = fopen("Q61_nation.json", "rb");
  int bytes = fread(json, 1, 4096, ji);
  assert(bytes < 4095);
  json[bytes] = 0;
  void* reader = riscv_create_reader(path, json);

  char* output = (char*)malloc(buffer_bytes);
  int rows = riscv_next_batch(reader, output);
  bytes = check_output(output, rows, 8, 8);
  printf("rows: %d, bytes: %d\n", rows, bytes);
  printf("\n");
}

void Q72_customer() {
  printf("Q72_customer ...\n");
  char path[256];
  strcpy(path, getenv("COMPSTOR"));
  int len = strlen(path);
  strcpy(path + len, "/tpch/tbl_s1e1/customer/00001.tbl");

  char json[4096];
  FILE* ji = fopen("Q72_customer.json", "rb");
  int bytes = fread(json, 1, 4096, ji);
  assert(bytes < 4095);
  json[bytes] = 0;
  void* reader = riscv_create_reader(path, json);

  char* output = (char*)malloc(buffer_bytes);
  int rows = riscv_next_batch(reader, output);
  bytes = check_output(output, rows, 8, 8);
  printf("rows: %d, bytes: %d\n", rows, bytes);
  printf("\n");
}

int main() {
  Q56_lineitem();
  Q61_nation();
  Q72_customer();
}