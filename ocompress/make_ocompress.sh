gcc -Wall -fPIC -O3 -c ocompress_filter.c
gcc  -fPIC -O3 -c avl.c rb.c bst.c splay.c minilzo.c
gcc -shared  -Wl,-soname,lib_ocompress.so.1 -o ocompress.so ocompress_filter.o avl.o rb.o bst.o splay.o minilzo.o
cp -f *.so /usr/lib64/tcmu-filters
