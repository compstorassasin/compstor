#ifndef RISCV_FILTER_H_
#define RISCV_FILTER_H_

#include <stdbool.h>

typedef struct _riscv_table_reader riscv_table_reader;
typedef struct _filter_desp filter_desp;

bool Or(riscv_table_reader*, filter_desp*);
bool int_EqualTo(riscv_table_reader*, filter_desp*);
bool int_NotEqualTo(riscv_table_reader*, filter_desp*);
bool int_GreaterThan(riscv_table_reader*, filter_desp*);
bool int_GreaterThanOrEqual(riscv_table_reader*, filter_desp*);
bool int_LessThan(riscv_table_reader*, filter_desp*);
bool int_LessThanOrEqual(riscv_table_reader*, filter_desp*);
bool str_EqualTo(riscv_table_reader*, filter_desp*);
bool str_NotEqualTo(riscv_table_reader*, filter_desp*);
bool str_GreaterThan(riscv_table_reader*, filter_desp*);
bool str_GreaterThanOrEqual(riscv_table_reader*, filter_desp*);
bool str_LessThan(riscv_table_reader*, filter_desp*);
bool str_LessThanOrEqual(riscv_table_reader*, filter_desp*);
bool str_StringStartsWith(riscv_table_reader*, filter_desp*);
bool str_NotStringStartsWith(riscv_table_reader*, filter_desp*);
bool str_StringEndsWith(riscv_table_reader*, filter_desp*);
bool str_NotStringEndsWith(riscv_table_reader*, filter_desp*);
bool str_StringContains(riscv_table_reader*, filter_desp*);
bool str_NotStringContains(riscv_table_reader*, filter_desp*);

#endif  // RISCV_FILTER_H_