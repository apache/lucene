Runs lucene microbenchmarks across a variety of CPUs in EC2.

Example:

```console
export AWS_ACCESS_KEY_ID=xxxxx
export AWS_SECRET_ACCESS_KEY=yyyy
make PATCH_BRANCH=rmuir:some-speedup
```

Results file will be in build/report.txt

You can also pass additional JMH args if you want:

```console
make PATCH_BRANCH=rmuir:some-speedup JMH_ARGS='float -p size=756'
```

Prerequisites:

1. It is expected that you have an ed25519 ssh key, use `ssh-keygen -t ed25519` to make one.
2. AWS key's IAM user needs `AmazonEC2FullAccess` and `AWSCloudFormationFullAccess` permissions at a minimum.
