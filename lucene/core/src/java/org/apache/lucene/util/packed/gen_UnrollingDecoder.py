#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
try:
  # python 3.9+
  from math import gcd
except ImportError:
  # old python
  from fractions import gcd

"""Code generation for bulk operations"""

BLOCK_SIZE = 128
BPVS = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56]
OUTPUT_FILE = "UnrollingDecoder.java"
HEADER = """// This file has been automatically generated, DO NOT EDIT

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.packed;

"""

def write_decoder(bpv, f):
  total_bits = bpv * BLOCK_SIZE
  long_count = total_bits / 64
  long_per_round = bpv / gcd(bpv, 64)
  total_round = long_count / long_per_round

  f.write('\n  static void decode%d(long[] src, long[] dst) {\n' % bpv)
  for round in range(0, total_round):
    write_round(bpv, long_per_round, round)
  f.write('  }\n')

def write_round(bpv, long_per_round, round):
  f.write('    {\n')
  for i in range(0, long_per_round):
    f.write('      long l%d = src[%d];\n' % (i, i + round * long_per_round))
  total_bits = long_per_round * 64
  dst_base = total_bits / bpv * round
  dst_pos = 0
  src_pos = 0
  remaining_bits = 64
  while total_bits > 0:
    shift = bpv * dst_pos % 64
    mask = 2**bpv - 1

    if remaining_bits > bpv:
      if shift == 0:
        f.write('      dst[%d] = l%d & %dL;\n' % (dst_pos + dst_base, src_pos, mask))
      else:
        f.write('      dst[%d] = (l%d >>> %d) & %dL;\n' % (dst_pos + dst_base, src_pos, shift, mask))
      remaining_bits = remaining_bits - bpv
    elif remaining_bits == bpv:
      f.write('      dst[%d] = l%d >>> %d;\n' % (dst_pos + dst_base, src_pos, shift))
    else:
      f.write('      dst[%d] = (l%d >>> %d) | ((l%d & %dL) << %d);\n' % (dst_pos + dst_base, src_pos, shift, src_pos + 1, pow(2, bpv - remaining_bits) - 1, remaining_bits))
      src_pos += 1
      remaining_bits = 64 - (bpv - remaining_bits)
    total_bits -= bpv
    dst_pos += 1
  f.write('    }\n')

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write('''/** Unrolling block decoder */\n''')

  f.write('class UnrollingDecoder {\n')

  for bpv in BPVS:
    write_decoder(bpv, f)

  f.write('}\n')
