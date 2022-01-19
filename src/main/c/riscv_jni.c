#include "riscv_jni.h"

#include "riscv_table_reader.h"
#include "riscvstorage_Native.h"

void *riscv_create_reader(const char *path, const char *json) {
#ifdef DEBUG
  printf("path: %s\njson: %s\n", path, json);
#endif
  return riscv_table_reader_init(path, json, NULL, NULL);
}
int riscv_next_batch(void *reader_ptr, char *output) {
  return riscv_table_reader_next_batch(reader_ptr, output);
}

JNIEXPORT jlong JNICALL Java_riscvstorage_Native_riscv_1create_1reader(
    JNIEnv *env, jclass cls, jstring path, jstring json) {
  const char *pathc = (*env)->GetStringUTFChars(env, path, 0);
  const char *jsonc = (*env)->GetStringUTFChars(env, json, 0);
  jlong ans = (jlong)riscv_create_reader(pathc, jsonc);
  (*env)->ReleaseStringUTFChars(env, path, pathc);
  (*env)->ReleaseStringUTFChars(env, json, jsonc);
  return ans;
}

JNIEXPORT jint JNICALL Java_riscvstorage_Native_riscv_1next_1batch(
    JNIEnv *env, jclass cls, jlong reader, jobject buffer) {
  char *buf = (char *)((*env)->GetDirectBufferAddress(env, buffer));
  jint ans = riscv_next_batch((void *)reader, buf);
  return ans;
}
