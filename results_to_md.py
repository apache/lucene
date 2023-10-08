import pickle

results = pickle.load(open('results.pk', 'rb'))

print('|NodeHash size|FST (mb)|RAM (mb)|Build time (sec)|')
print('|-------------|--------|--------|----------------|')
for size, fst_mb, fst_build_sec, ram_mb in results:
  print(f'|{size}|{fst_mb:.1f}|{ram_mb:.1f}|{fst_build_sec:.1f}|')
