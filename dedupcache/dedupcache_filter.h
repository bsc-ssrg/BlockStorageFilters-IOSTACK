/* flags.h – Header file */
#include <stdbool.h>
void print_bind_msg(char *msg);
int get_flag();
void set_flag(int flag);
void write_xform(void* buf, unsigned long cnt, unsigned long int offset);
void read_xform(void* buf, unsigned long cnt, unsigned long int offset);	// Stores the content in 4K blocks
int pre_read(void* buf, unsigned long cnt, unsigned long int offset,unsigned int fdesc, bool * doRead);
