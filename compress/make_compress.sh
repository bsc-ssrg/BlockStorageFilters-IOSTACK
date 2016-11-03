gcc -Wall -fPIC -c compress_filter.c
gcc -fPIC -c avl.c bst.c rb.c splay.c minilzo.c
gcc -shared  -Wl,-soname,lib_compress.so.1 -o compress.so compress_filter.o bst.o avl.o rb.o splay.o minilzo.o
cp -f *.so /usr/lib64/tcmu-filters
