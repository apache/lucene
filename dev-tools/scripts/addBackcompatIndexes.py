#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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


# For usage information, see:
# 
#   http://wiki.apache.org/lucene-java/ReleaseTodo#Generate_Backcompat_Indexes


import os
import sys
sys.path.append(os.path.dirname(__file__))
import scriptutil

import argparse
import urllib.error
import urllib.request
import re
import shutil

def create_and_add_index(source, indextype, index_version, current_version, temp_dir):
  if not current_version.is_back_compat_with(index_version):
    prefix = 'unsupported'
  else:
    prefix = {
      'cfs': 'index',
      'nocfs': 'index',
      'sorted': 'sorted',
      'int8_hnsw': 'int8_hnsw',
      'moreterms': 'moreterms',
      'dvupdates': 'dvupdates',
      'emptyIndex': 'empty'
    }[indextype]
  if indextype in ('cfs', 'nocfs'):
    filename = '%s.%s-%s.zip' % (prefix, index_version, indextype)
  else:
    filename = '%s.%s.zip' % (prefix, index_version)
  
  print('  creating %s...' % filename, end='', flush=True)
  module = 'backward-codecs'
  index_dir = os.path.join('lucene', module, 'src/test/org/apache/lucene/backward_index')
  if os.path.exists(os.path.join(index_dir, filename)):
    print('uptodate')
    return

  test = {
    'cfs': 'testCreateCFS',
    'nocfs': 'testCreateNoCFS',
    'sorted': 'testCreateSortedIndex',
    'int8_hnsw': 'testCreateInt8HNSWIndices',
    'moreterms': 'testCreateMoreTermsIndex',
    'dvupdates': 'testCreateIndexWithDocValuesUpdates',
    'emptyIndex': 'testCreateEmptyIndex'
  }[indextype]
  gradle_args = ' '.join([
    '-Ptests.useSecurityManager=false',
    '-p lucene/%s' % module,
    'test',
    '--tests TestGenerateBwcIndices.%s' % test,
    '-Dtests.bwcdir=%s' % temp_dir,
    '-Dtests.codec=default'
  ])
  base_dir = os.getcwd()
  bc_index_file = os.path.join(temp_dir, filename)
  
  if os.path.exists(bc_index_file):
    print('alreadyexists')
  else:
    os.chdir(source)
    scriptutil.run('./gradlew %s' % gradle_args)
    if not os.path.exists(bc_index_file):
      raise Exception("Expected file can't be found: %s" %bc_index_file)
    print('done')
  
  print('  adding %s...' % filename, end='', flush=True)
  scriptutil.run('cp %s %s' % (bc_index_file, os.path.join(base_dir, index_dir)))
  os.chdir(base_dir)
  print('done')

def update_backcompat_tests(index_version, current_version):
  print('  adding new indexes to backcompat tests...', end='', flush=True)
  module = 'lucene/backward-codecs'

  filename = None
  if not current_version.is_back_compat_with(index_version):
    filename = '%s/src/test/org/apache/lucene/backward_index/unsupported_versions.txt' % module
  else:
    filename = '%s/src/test/org/apache/lucene/backward_index/versions.txt' % module

  strip_dash_suffix_re = re.compile(r'-.*')

  def find_version(x):
    x = x.strip()
    x = re.sub(strip_dash_suffix_re, '', x) # remove the -suffix if any
    return scriptutil.Version.parse(x)

  def edit(buffer, match, line):
    v = find_version(line)
    changed = False
    if v.on_or_after(index_version):
       if not index_version.on_or_after(v):
         buffer.append(('%s\n') % index_version)
       changed = True
    buffer.append(line)
    return changed

  def append(buffer, changed):
    if changed:
      return changed
    if not buffer[len(buffer)-1].endswith('\n'):
      buffer.append('\n')
    buffer.append(('%s\n') % index_version)
    return True
        
  changed = scriptutil.update_file(filename, re.compile(r'.*'), edit, append)
  print('done' if changed else 'uptodate')

def check_backcompat_tests():
  print('  checking backcompat tests...', end='', flush=True)
  scriptutil.run('./gradlew -p lucene/backward-codecs test --tests TestGenerateBwcIndices')
  print('ok')

def download_from_cdn(version, remotename, localname):
  url = 'http://dlcdn.apache.org/lucene/java/%s/%s' % (version, remotename)
  try:
    urllib.request.urlretrieve(url, localname)
    return True
  except urllib.error.URLError as e:
    if e.code == 404:
      return False
    raise e

def download_from_archives(version, remotename, localname):
  url = 'http://archive.apache.org/dist/lucene/java/%s/%s' % (version, remotename)
  try:
    urllib.request.urlretrieve(url, localname)
    return True
  except urllib.error.URLError as e:
    if e.code == 404:
      return False
    raise e

def download_release(version, temp_dir, force):
  print('  downloading %s source release...' % version, end='', flush=True)
  source = os.path.join(temp_dir, 'lucene-%s' % version) 
  if os.path.exists(source):
    if force:
      shutil.rmtree(source)
    else:
      print('uptodate')
      return source

  filename = 'lucene-%s-src.tgz' % version
  source_tgz = os.path.join(temp_dir, filename)
  if not download_from_cdn(version, filename, source_tgz) and \
     not download_from_archives(version, filename, source_tgz):
    raise Exception('Could not find version %s in apache CDN or archives' % version)

  olddir = os.getcwd()
  os.chdir(temp_dir)
  scriptutil.run('tar -xvzf %s' % source_tgz)
  os.chdir(olddir) 
  print('done')
  return source

def read_config():
  parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                   description='''\
Add backcompat index and test for new version.  See:
http://wiki.apache.org/lucene-java/ReleaseTodo#Generate_Backcompat_Indexes
''')
  parser.add_argument('--force', action='store_true', default=False,
                      help='Redownload the version and rebuild, even if it already exists')
  parser.add_argument('--no-cleanup', dest='cleanup', action='store_false', default=True,
                      help='Do not cleanup the built indexes, so that they can be reused ' +
                           'for adding to another branch')
  parser.add_argument('--temp-dir', metavar='DIR', default='/tmp/lucenebwc',
                      help='Temp directory to build backcompat indexes within')
  parser.add_argument('version', type=scriptutil.Version.parse,
                      help='Version to add, of the form X.Y.Z')
  c = parser.parse_args()

  return c
  
def main():
  c = read_config() 
  if not os.path.exists(c.temp_dir):
    os.makedirs(c.temp_dir)

  print('\nCreating backwards compatibility indexes')
  source = download_release(c.version, c.temp_dir, c.force)
  current_version = scriptutil.Version.parse(scriptutil.find_current_version())
  create_and_add_index(source, 'cfs', c.version, current_version, c.temp_dir)
  create_and_add_index(source, 'nocfs', c.version, current_version, c.temp_dir)
  create_and_add_index(source, 'int8_hnsw', c.version, current_version, c.temp_dir)
  should_make_sorted =     current_version.is_back_compat_with(c.version) \
                       and (c.version.major > 6 or (c.version.major == 6 and c.version.minor >= 2))
  if should_make_sorted:
    create_and_add_index(source, 'sorted', c.version, current_version, c.temp_dir)
  if c.version.minor == 0 and c.version.bugfix == 0 and current_version.is_back_compat_with(c.version):
    create_and_add_index(source, 'moreterms', c.version, current_version, c.temp_dir)
    create_and_add_index(source, 'dvupdates', c.version, current_version, c.temp_dir)
    create_and_add_index(source, 'emptyIndex', c.version, current_version, c.temp_dir)
    print ('\nMANUAL UPDATE REQUIRED: edit TestGenerateBwcIndices to enable moreterms, dvupdates, and empty index testing')
    
  print('\nAdding backwards compatibility tests')
  update_backcompat_tests(c.version, current_version)


  print('\nTesting changes')
  check_backcompat_tests()

  if c.cleanup:
    print('\nCleaning up')
    print('  deleting %s...' % c.temp_dir, end='', flush=True)
    shutil.rmtree(c.temp_dir)
    print('done')

  print()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('\nRecieved Ctrl-C, exiting early')
