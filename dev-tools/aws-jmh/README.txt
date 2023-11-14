Runs lucene microbenchmarks across a variety of CPUs in EC2.

Example:

export AWS_ACCESS_KEY_ID=xxxxx
export AWS_SECRET_ACCESS_KEY=yyyy
make PATCH_BRANCH=rmuir:some-speedup

Results file will be in build/report.txt

You can also pass additional JMH args if you want:

make PATCH_BRANCH=rmuir:some-speedup JMH_ARGS='float -p size=756'
