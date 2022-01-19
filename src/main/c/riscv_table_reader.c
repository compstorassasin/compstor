#include "riscv_table_reader.h"

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jsmn/jsmn.h"
#include "riscv_filter.h"
#include "riscv_producer.h"

void (*riscv_producers[])(riscv_table_reader*) = {
    int_producer, date_producer, decimal_producer, str_producer, scanner};

bool (*riscv_filters[])(riscv_table_reader*, filter_desp*) = {
    NULL,
    Or,
    int_EqualTo,
    int_NotEqualTo,
    int_GreaterThan,
    int_GreaterThanOrEqual,
    int_LessThan,
    int_LessThanOrEqual,
    str_EqualTo,
    str_NotEqualTo,
    str_GreaterThan,
    str_GreaterThanOrEqual,
    str_LessThan,
    str_LessThanOrEqual,
    str_StringStartsWith,
    str_NotStringStartsWith,
    str_StringEndsWith,
    str_NotStringEndsWith,
    str_StringContains,
    str_NotStringContains};

riscv_table_reader* riscv_table_reader_init(const char* path, const char* json,
                                            riscv_table_reader* reader,
                                            char* input_buffer) {
  if (!reader) {
    reader = (riscv_table_reader*)malloc(sizeof(riscv_table_reader));
    reader->free_reader = true;
  } else {
    reader->free_reader = false;
  }
  {
    FILE* file = fopen(path, "rb");
    fseek(file, 0L, SEEK_END);
    register int bytes = ftell(file);
    rewind(file);

    if (!input_buffer) {
      reader->input_buffer = (char*)malloc(bytes);
      reader->free_input_buffer = true;
    } else {
      reader->input_buffer = input_buffer;
      reader->free_input_buffer = false;
    }

    register int rbytes = fread(reader->input_buffer, 1, bytes, file);
    fclose(file);
    printf("ibytes %d\n", bytes);
    assert(bytes == rbytes);
    reader->bytes = bytes;

    reader->input = reader->input_buffer;
  }

  reader->filters[0].filter = NULL;
  reader->childfilters[0].filter = NULL;
  int pf = 1;
  int pc = 1;
  char* sp = reader->strref;

  jsmn_parser parser;
  jsmntok_t t[MAX_NUM_TOKENS];

  jsmn_init(&parser);
  int tokens = jsmn_parse(&parser, json, strlen(json), t, MAX_NUM_TOKENS);
  assert(tokens >= 0);
  assert(JSMN_ARRAY == t[0].type);
  int p = 1;
  reader->fields = t[0].size;
  reader->rfields = 0;
  for (int i = 0; i < t[0].size; i++) {
    reader->filter_start[i] = 0;
    assert(JSMN_OBJECT == t[p].type);
    int fds = t[p].size;
    p += 1;
    for (int j = 0; j < fds; j++) {
      assert(JSMN_STRING == t[p].type);
      char fch = json[t[p++].start];
      if (fch == 'n') {
        p++;
      } else if (fch == 'r') {
        reader->required[i] = ('t' == json[t[p].start]);
        reader->rfields += reader->required[i] ? 1 : 0;
        p++;
      } else if (fch == 't') {
        assert(t[p].end == t[p].start + 1);
        reader->producers[i] = riscv_producers[json[t[p].start] - '0'];
        p++;
      } else if (fch == 'f') {
        assert(JSMN_ARRAY == t[p].type);
        int nf = t[p].size;
        p++;
        for (int k = 0; k < nf; k++) {
          assert(JSMN_OBJECT == t[p].type);
          assert(1 == t[p].size);

          p += 1;
          assert(JSMN_STRING == t[p].type);
          int ft = 0;
          for (const char* l = json + t[p].start; l < json + t[p].end; l++) {
            assert((*l >= '0') && (*l) <= '9');
            ft *= 10;
            ft += (*l - '0');
          }

          p += 1;
          reader->filters[pf].filter = riscv_filters[ft];
          if (k == 0) {
            reader->filter_start[i] = pf;
          }
          if (ft == 1) {
            assert(JSMN_ARRAY == t[p].type);
            int cof = t[p].size;
            p += 1;

            reader->filters[pf].val = pc;
            for (int o = 0; o < cof; o++) {
              assert(JSMN_OBJECT == t[p].type);
              p += 1;
              assert(JSMN_STRING == t[p].type);
              int cft = 0;
              for (const char* l = json + t[p].start; l < json + t[p].end;
                   l++) {
                assert((*l >= '0') && (*l) <= '9');
                cft *= 10;
                cft += (*l - '0');
              }
              reader->childfilters[pc].filter = riscv_filters[cft];
              p += 1;
              if (cft <= 7) {
                assert(JSMN_PRIMITIVE == t[p].type);
                reader->childfilters[pc].val = 0;
                for (const char* l = json + t[p].start; l < json + t[p].end;
                     l++) {
                  assert((*l >= '0') && (*l) <= '9');
                  reader->childfilters[pc].val *= 10;
                  reader->childfilters[pc].val += (*l - '0');
                }
              } else {
                int bytes = t[p].end - t[p].start;
                strncpy(sp, json + t[p].start, bytes);
                reader->childfilters[pc].str = sp;
                sp += bytes;
                *(sp++) = 0;
              }
              pc++;
              p += 1;
            }
            reader->childfilters[pc++].filter = NULL;
          } else if (ft <= 7) {
            assert(JSMN_PRIMITIVE == t[p].type);
            reader->filters[pf].val = 0;
            for (const char* l = json + t[p].start; l < json + t[p].end; l++) {
              reader->filters[pf].val *= 10;
              reader->filters[pf].val += (*l - '0');
            }
            p += 1;
          } else {
            int bytes = t[p].end - t[p].start;
            strncpy(sp, json + t[p].start, bytes);
            reader->filters[pf].str = sp;
            sp += bytes;
            *(sp++) = 0;
            p += 1;
          }
          pf++;
        }
        if (nf > 0) {
          reader->filters[pf++].filter = NULL;
        }

        assert(pf <= MAX_NUM_FILTERS);
        assert(pc <= MAX_NUM_FILTERS);
      }
    }
  }
  return reader;
}

int riscv_table_reader_next_batch(riscv_table_reader* reader, char* output) {
  const char* input_end = reader->input_buffer + reader->bytes;
  const char* output_ref = output;
  int rows = 0;
  int row_size = 8 + (reader->rfields << 3);

  while (reader->input != input_end) {
    reader->row_null = output + 8;
    register int* out = (int*)reader->row_null;
    reader->this_str = output + row_size;
    register bool valid = true;

    for (int i = 0; i < reader->fields; i++) {
      char* str = reader->this_str;
      reader->producers[i](reader);
      filter_desp* pf = reader->filters + reader->filter_start[i];
      while (pf->filter && valid) {
        valid &= pf->filter(reader, pf);
        pf++;
      }
      if (!valid) break;
      if (reader->required[i]) {
        *(out++) = reader->first_int;
        *(out++) = reader->second_int;
      } else {
        reader->this_str = str;
      }
    }
    if (valid) {
      rows += 1;
      assert('\n' == *reader->input);
      reader->input++;
      int* outsize = (int*)output;
      *(outsize++) = 0;
      *outsize = reader->this_str - output - 8;
      output = reader->this_str;
    } else {
      while ('\n' != *reader->input) {
        reader->input++;
      }
      reader->input++;
    }
  }

  printf("obytes %ld\n", output - output_ref);
  riscv_table_reader_close(reader);
  return rows;
}

void riscv_table_reader_close(riscv_table_reader* reader) {
  if (reader->free_input_buffer) free(reader->input_buffer);
  if (reader->free_reader) free(reader);
}
