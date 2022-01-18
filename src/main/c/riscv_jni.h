#ifndef RISCV_JNI_H_
#define RISCV_JNI_H_

void *riscv_create_reader(const char *path, const char *json);
int riscv_next_batch(void *reader_ptr, char *output);

#endif  // RISCV_JNI_H_