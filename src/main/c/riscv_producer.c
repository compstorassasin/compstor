#include "riscv_producer.h"

#include <assert.h>

#include "riscv_table_reader.h"

void int_producer(riscv_table_reader* reader) {
  register const char* input = reader->input;
  register int fint = 0;
  for (; *input != field_delimiter; input++) {
    fint *= 10;
    fint += (*input - '0');
  }
  reader->first_int = fint;
  reader->second_int = 0;
  reader->input = input + 1;
}

static const int month_days[] = {0, 31,  59,  90,  120, 151, 181,
                                 212, 243, 273, 304, 334, 365};
void date_producer(riscv_table_reader* reader) {
  register const char* input = reader->input;
  register int days = 0;
  register int year_diff = 10;
  register int month = 0;

  if ('1' == *input) {
    input++;
    assert('9' == *input);
    input++;
    year_diff = (*(input++)) - '7';
  } else {
    assert('2' == *input);
    input++;
    assert('0' == *input);
    input++;
    year_diff += (*(input++)) - '7';
  }
  year_diff *= 10;
  year_diff += (*(input++)) - '0';
  days += (year_diff)*365;

  assert(date_delimiter == *input);
  input++;

  assert(('0' == *input) || ('1' == *input));
  month = (*(input++)) - '0';
  month *= 10;
  month += (*(input++)) - '1';
  days += month_days[month];

  year_diff += (month > 1) ? 2 : 1;  // for lear year. diff is from 1970
  days += (year_diff >> 2);  // There's no 1900/2100/... between 1970 and 2099.

  assert(date_delimiter == *input);
  input++;
  // reuse month register for parsing day;
  month = (*(input++)) - '0';
  month *= 10;
  month += (*(input++)) - '1';
  days += month;

  assert(field_delimiter == *input);

  reader->first_int = days;
  reader->second_int = 0;
  reader->input = input + 1;
}

void decimal_producer(riscv_table_reader* reader) {
  register const char* input = reader->input;
  register int fint = 0;

  for (; *input != field_delimiter && *input != '.'; input++) {
    fint *= 10;
    fint += (*input) - '0';
  }
  if (*input == field_delimiter) {
    fint *= 100;
  } else {
    input++;
    register int i = 2;
    for (; *input != field_delimiter; input++) {
      fint *= 10;
      fint += (*input) - '0';
      i--;
    }
    assert(i >= 0);
    if (i == 2) {
      fint *= 100;
    } else if (i == 1) {
      fint *= 10;
    }
  }

  reader->first_int = fint;
  reader->second_int = 0;
  reader->input = input + 1;
}

void str_producer(riscv_table_reader* reader) {
  register const char* input = reader->input;
  register char* str = reader->this_str;
  register int sint = 0;

  for (; *input != field_delimiter; input++) {
    *(str++) = *input;
    sint++;
  }
  *str = 0;

  reader->first_int = str - reader->row_null - sint + 8;
  reader->second_int = sint;
  reader->this_str = str;
  reader->input = input + 1;
}

void scanner(riscv_table_reader* reader) {
  register const char* input = reader->input;
  for (; *input != field_delimiter; input++) {
  }
  reader->input = input + 1;
}
