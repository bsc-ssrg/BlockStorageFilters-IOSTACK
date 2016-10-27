gcc -fPIC -c  -O3 dedupcache_filter.c
gcc -fPIC -c -O3 avl.c bst.c rb.c splay.c 
gcc -shared  -Wl,-soname,lib_dedupcache.so.1 -o dedupcache.so dedupcache_filter.o bst.o avl.o rb.o splay.o 
cp -f *.so /usr/lib64/tcmu-filters
