gcc -Wall -fPIC -c prefetch_filter.c
gcc -shared -Wl,-soname,lib_prefetch.so.1 -o prefetch.so prefetch_filter.o
cp -f *.so /usr/lib64/tcmu-filters