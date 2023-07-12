dpu-clang -I ../inc/ test_index_basic.c ../src/decoder.c ../src/matcher.c ../src/parser.c ../src/query_parser.c ../src/term_lookup.c -o test_index_basic
dpu-clang -I ../inc/ test_index_moretext.c ../src/decoder.c ../src/matcher.c ../src/parser.c ../src/query_parser.c ../src/term_lookup.c -o test_index_moretext
clang -O3 test.c -o test -I/usr/include/dpu -ldpu
./test
