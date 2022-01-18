#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

extern void sync(void);

int main(void) {
    if (geteuid() != 0) {
        fprintf(stderr, "flush-cache: Not root\n");
        exit(EXIT_FAILURE);
    }
    printf("Flushing page cache, dentries and inodes...\n");
    // First: the traditional three sync calls. Perhaps not needed?
    // For security reasons, system("sync") is not a good idea.
    sync();
    sync();
    sync();
    FILE* f;
    f = fopen("/proc/sys/vm/drop_caches", "w");
    if (f == NULL) {
        fprintf(stderr, "flush-cache: Couldn't open /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }
    if (fprintf(f, "3\n") != 2) {
        fprintf(stderr, "flush-cache: Couldn't write 3 to /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }
    fclose(f);
    printf("Done flushing.\n");

    return 0;
}
