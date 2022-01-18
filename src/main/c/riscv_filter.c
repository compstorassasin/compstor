#include "riscv_filter.h"

#include <string.h>

#include "riscv_table_reader.h"

bool Or(riscv_table_reader* reader, filter_desp* desp) {
  register filter_desp* fp = reader->childfilters + desp->val;
  register bool valid = false;
  while (fp->filter != NULL) {
    valid |= fp->filter(reader, fp);
    fp++;
  }
  return valid;
}

bool int_EqualTo(riscv_table_reader* reader, filter_desp* desp) {
  return (reader->first_int == desp->val);
}
bool int_NotEqualTo(riscv_table_reader* reader, filter_desp* desp) {
  return !(reader->first_int == desp->val);
}
bool int_GreaterThan(riscv_table_reader* reader, filter_desp* desp) {
  return (reader->first_int > desp->val);
}
bool int_GreaterThanOrEqual(riscv_table_reader* reader, filter_desp* desp) {
  return (reader->first_int >= desp->val);
}
bool int_LessThan(riscv_table_reader* reader, filter_desp* desp) {
  return (reader->first_int < desp->val);
}
bool int_LessThanOrEqual(riscv_table_reader* reader, filter_desp* desp) {
  return (reader->first_int <= desp->val);
}

bool str_EqualTo(riscv_table_reader* reader, filter_desp* desp) {
  return (0 == strcmp(reader->this_str - reader->second_int, desp->str));
}
bool str_NotEqualTo(riscv_table_reader* reader, filter_desp* desp) {
  return (0 != strcmp(reader->this_str - reader->second_int, desp->str));
}
bool str_GreaterThan(riscv_table_reader* reader, filter_desp* desp) {
  return (strcmp(reader->this_str - reader->second_int, desp->str) > 0);
}
bool str_GreaterThanOrEqual(riscv_table_reader* reader, filter_desp* desp) {
  return (strcmp(reader->this_str - reader->second_int, desp->str) >= 0);
}
bool str_LessThan(riscv_table_reader* reader, filter_desp* desp) {
  return (strcmp(reader->this_str - reader->second_int, desp->str) < 0);
}
bool str_LessThanOrEqual(riscv_table_reader* reader, filter_desp* desp) {
  return (strcmp(reader->this_str - reader->second_int, desp->str) <= 0);
}
bool str_StringStartsWith(riscv_table_reader* reader, filter_desp* desp) {
  char* start = reader->this_str - reader->second_int;
  return (strstr(start, desp->str) == start);
}
bool str_NotStringStartsWith(riscv_table_reader* reader, filter_desp* desp) {
  char* start = reader->this_str - reader->second_int;
  return (strstr(start, desp->str) != start);
}
bool str_StringEndsWith(riscv_table_reader* reader, filter_desp* desp) {
  return (strstr(reader->this_str - reader->second_int, desp->str) +
              strlen(desp->str) ==
          reader->this_str);
}
bool str_NotStringEndsWith(riscv_table_reader* reader, filter_desp* desp) {
  return (strstr(reader->this_str - reader->second_int, desp->str) +
              strlen(desp->str) !=
          reader->this_str);
}
bool str_StringContains(riscv_table_reader* reader, filter_desp* desp) {
  return (strstr(reader->this_str - reader->second_int, desp->str) != NULL);
}
bool str_NotStringContains(riscv_table_reader* reader, filter_desp* desp) {
  return (strstr(reader->this_str - reader->second_int, desp->str) == NULL);
}
