gcc -Wall -fPIC -c mockup_filter.c
gcc -shared  -Wl,-soname,lib_mockup.so.1 -o mockup.so mockup_filter.o 
cp -f *.so /usr/lib64/tcmu-filters
