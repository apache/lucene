# nocommit -- temporary tool

import time
import subprocess
import re
import pickle

size = 0

print('Build jars...')
subprocess.check_call(['./gradlew', 'jar'])

print('Build IndexToFST')
subprocess.check_call('javac -cp lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar IndexToFST.java', shell=True)

results = []

while True:

  print(f'\nTest size={size}')
  stdout = subprocess.check_output(f'java -cp .:lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar IndexToFST /l/indices/wikimediumall.trunk.facets.taxonomy:Date.taxonomy:Month.taxonomy:DayOfYear.taxonomy:RandomLabel.taxonomy.sortedset:Date.sortedset:Month.sortedset:DayOfYear.sortedset:RandomLabel.sortedset.Lucene90.Lucene90.dvfields.nd33.3326M/index {size}', shell=True)

  stdout = stdout.decode('utf-8')
  
  m = re.search('^saved FST to "fst.bin": (\d+) bytes; ([0-9.]+) sec$', stdout, re.MULTILINE)
  fst_mb = int(m.group(1))/1024/1024
  fst_build_sec = float(m.group(2))

  # double barrel, long[size] -- note we can do better (using packed paged map like main):
  ram_mb = (size * 8 * 2) / 1024 / 1024
  
  print(f'{size}: {ram_mb:.2f} MB, {fst_build_sec:.3f} sec')
  fst_build_sec = float(fst_build_sec)

  results.append((size, fst_mb, fst_build_sec, ram_mb))

  pickle.dump(results, open('results.pk', 'wb'))

  print(f'hash_size,fst_mb,fst_build_sec,ram_mb')
  for size, fst_mb, fst_build_sec, ram_mb in results:
    print(f'{size},{fst_mb:.3f},{fst_build_sec:.3f},{ram_mb:.3f}')

  # don't test beyond 1 GB
  if size == 1073741824:
    break

  if size == 0:
    size = 4
  else:
    size *= 2
