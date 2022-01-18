#ifndef RISCV_PRODUCER_H_
#define RISCV_PRODUCER_H_

typedef struct _riscv_table_reader riscv_table_reader;

static const char row_delimiter = '\n';
static const char field_delimiter = '|';
static const char date_delimiter = '-';

void int_producer(riscv_table_reader* reader);
void date_producer(riscv_table_reader* reader);
void decimal_producer(riscv_table_reader* reader);
void str_producer(riscv_table_reader* reader);
void scanner(riscv_table_reader* reader);

#endif  // RISCV_PRODUCER_H_