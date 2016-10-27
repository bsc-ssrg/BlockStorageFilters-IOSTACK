gcc -Wall -fPIC -c prefetch2_filter.c
gcc -fPIC -c avl.c bst.c rb.c splay.c
gcc -shared  -Wl,-soname,lib_prefetch2.so.1 -o prefetch2.so prefetch2_filter.o bst.o avl.o rb.o splay.o
cp -f *.so /usr/lib64/tcmu-filters
