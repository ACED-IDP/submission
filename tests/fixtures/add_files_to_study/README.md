To create large file
cat /dev/urandom | head -c  `echo "1073741824 * 1" | bc`  > tests/fixtures/dir_to_study/file-0.txt
