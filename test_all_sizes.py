# nocommit -- temporary tool

import time
import subprocess
import re
import pickle

print('Build jars...')
subprocess.check_call(['./gradlew', 'jar'])

print('Build IndexToFST')
subprocess.check_call('javac -cp lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar IndexToFST.java', shell=True)

if True:
  results = []
  ram_mb = float('+inf')
else:
  results = pickle.load(open('results.pk', 'rb'))
  results.sort()
  ram_mb = results[-2][0] + 4

max_actual = None

while True:

  print(f'\nTest ram_mb={ram_mb}')
  stdout = subprocess.check_output(f'java -cp .:lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar IndexToFST /l/indices/wikimediumall.trunk.facets.taxonomy:Date.taxonomy:Month.taxonomy:DayOfYear.taxonomy:RandomLabel.taxonomy.sortedset:Date.sortedset:Month.sortedset:DayOfYear.sortedset:RandomLabel.sortedset.Lucene90.Lucene90.dvfields.nd33.3326M/index {ram_mb}', shell=True)

  stdout = stdout.decode('utf-8')
  open(f'logs/{ram_mb}.stdout', 'w', encoding='utf-8').write(stdout)
  
  m = re.search('^saved FST to "fst.bin": (\d+) bytes; ([0-9.]+) sec$', stdout, re.MULTILINE)
  fst_mb = int(m.group(1))/1024/1024
  fst_build_sec = float(m.group(2))

  actual_ram_mb = 0
  
  for s in re.findall('RAM (\d+) bytes$', stdout, re.MULTILINE):
    actual_ram_mb = max(actual_ram_mb, int(s) / 1024 / 1024)

  if actual_ram_mb > ram_mb:
    print(f'WARNING: ram_mb={ram_mb} but actual_ram_mb={actual_ram_mb}')
    
  print(f'{ram_mb}: {actual_ram_mb:.2f} MB, {fst_build_sec:.3f} sec')
  fst_build_sec = float(fst_build_sec)

  results.append((ram_mb, fst_mb, fst_build_sec, actual_ram_mb))
  results.sort()

  pickle.dump(results, open('results.pk', 'wb'))

  print(f'ram_mb,fst_mb,fst_build_sec,actual_ram_mb')
  for ram_mb0, fst_mb, fst_build_sec, actual_ram_mb in results:
    print(f'{ram_mb0},{fst_mb:.3f},{fst_build_sec:.3f},{actual_ram_mb:.3f}')

  if ram_mb == float('+inf'):
    ram_mb = 0
    max_actual = actual_ram_mb
  else:
    ram_mb += 4
    
  if ram_mb > 512 or (False and ram_mb > 0 and actual_ram_mb == max_actual):
    break
