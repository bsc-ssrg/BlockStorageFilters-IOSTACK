/* flags.h – Header file */
#include <stdbool.h>

void print_bind_msg(char *msg);
int get_flag();
void set_flag(int flag);
void write_xform(void* buf, unsigned long cnt, unsigned long int offset, bool *doWrite);
void read_xform(void* buf, unsigned long cnt, unsigned long intoffset);
int pre_read(void* buf, unsigned long cnt, unsigned long int offset,unsigned int fdesc, bool * doRead);