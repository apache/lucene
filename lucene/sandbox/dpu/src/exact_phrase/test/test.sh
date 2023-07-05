dpu-clang test_index_basic.c -o test_index_basic
dpu-clang test_index_moretext.c -o test_index_moretext
clang -O3 test.c -o test -I/usr/include/dpu -ldpu
./test
