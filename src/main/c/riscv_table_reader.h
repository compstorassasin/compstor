#ifndef RISCV_TABLE_READER_H
#define RISCV_TABLE_READER_H

#include <stdbool.h>
#include <stdio.h>

#define MAX_NUM_FIELDS 32
#define MAX_NUM_FILTERS 32
#define MAX_NUM_TOKENS 1024
#define MAX_STR_LEN 256

typedef struct _riscv_table_reader riscv_table_reader;
typedef struct _filter_desp filter_desp;

struct _filter_desp {
  bool (*filter)(riscv_table_reader*, filter_desp*);
  char* str;
  int val;
};

struct _riscv_table_reader {
  bool required[MAX_NUM_FIELDS];
  void (*producers[MAX_NUM_FIELDS])(riscv_table_reader*);
  char filter_start[MAX_NUM_FIELDS];
  filter_desp filters[MAX_NUM_FILTERS];
  filter_desp childfilters[MAX_NUM_FILTERS];
  char strref[MAX_STR_LEN];

  int fields;
  int rfields;
  int bytes;
  char* input_buffer;
  const char* input;

  char* row_null;
  char* this_str;
  int first_int;
  int second_int;
};

riscv_table_reader* riscv_table_reader_init(const char* path, const char* json);
int riscv_table_reader_next_batch(riscv_table_reader* reader, char* output);
void riscv_table_reader_close(riscv_table_reader* reader);

#endif  // RISCV_TABLE_READER_H