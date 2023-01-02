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

import argparse
import datetime
import re
import time
import os
import sys
import subprocess
from subprocess import TimeoutExpired
import textwrap
import urllib.request, urllib.error, urllib.parse
import xml.etree.ElementTree as ET

import scriptutil

LOG = '/tmp/release.log'
dev_mode = False

def log(msg):
  f = open(LOG, mode='ab')
  f.write(msg.encode('utf-8'))
  f.close()

def run(command):
  log('\n\n%s: RUN: %s\n' % (datetime.datetime.now(), command))
  if os.system('%s >> %s 2>&1' % (command, LOG)):
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    print(msg)
    raise RuntimeError(msg)


def runAndSendGPGPassword(command, password):
  p = subprocess.Popen(command, shell=True, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
  f = open(LOG, 'ab')
  while True:
    p.stdout.flush()
    line = p.stdout.readline()
    if len(line) == 0:
      break
    f.write(line)
    if line.find(b'Enter GPG keystore password:') != -1:
      time.sleep(1.0)
      p.stdin.write((password + '\n').encode('UTF-8'))
      p.stdin.write('\n'.encode('UTF-8'))

  try:
    result = p.wait(timeout=120)
    if result != 0:
      msg = '    FAILED: %s [see log %s]' % (command, LOG)
      print(msg)
      raise RuntimeError(msg)
  except TimeoutExpired:
    msg = '    FAILED: %s [timed out after 2 minutes; see log %s]' % (command, LOG)
    print(msg)
    raise RuntimeError(msg)

def load(urlString, encoding="utf-8"):
  try:
    content = urllib.request.urlopen(urlString).read().decode(encoding)
  except Exception as e:
    print('Retrying download of url %s after exception: %s' % (urlString, e))
    content = urllib.request.urlopen(urlString).read().decode(encoding)
  return content

def getGitRev():
  if not dev_mode:
    status = os.popen('git status').read().strip()
    if 'nothing to commit, working directory clean' not in status and 'nothing to commit, working tree clean' not in status:
      raise RuntimeError('git clone is dirty:\n\n%s' % status)
    if 'Your branch is ahead of' in status:
      raise RuntimeError('Your local branch is ahead of the remote? git status says:\n%s' % status)
    print('  git clone is clean')
  else:
    print('  Ignoring dirty git clone due to dev-mode')
  return os.popen('git rev-parse HEAD').read().strip()


def prepare(root, version, gpg_key_id, gpg_password, gpg_home=None, sign_gradle=False):
  print()
  print('Prepare release...')
  if os.path.exists(LOG):
    os.remove(LOG)

  if not dev_mode:
    os.chdir(root)
    print('  git pull...')
    run('git pull')
  else:
    print('  Development mode, not running git pull')

  rev = getGitRev()
  print('  git rev: %s' % rev)
  log('\nGIT rev: %s\n' % rev)

  print('  Check DOAP files')
  checkDOAPfiles(version)

  if not dev_mode:
    print('  ./gradlew --no-daemon -Dtests.badapples=false clean check')
    run('./gradlew --no-daemon -Dtests.badapples=false clean check')
  else:
    print('  skipping precommit check due to dev-mode')

  print('  prepare-release')
  cmd = './gradlew --no-daemon assembleRelease' \
        ' -Dversion.release=%s' % version
  if dev_mode:
    cmd += ' -Pvalidation.git.failOnModified=false'
  if gpg_key_id is not None:
    cmd += ' -Psign --max-workers 2'
    if sign_gradle:
      print("  Signing method is gradle java-plugin")
      cmd += ' -Psigning.keyId="%s"' % gpg_key_id
      if gpg_home is not None:
        cmd += ' -Psigning.secretKeyRingFile="%s"' % os.path.join(gpg_home, 'secring.gpg')
      if gpg_password is not None:
        # Pass gpg passphrase as env.var to gradle rather than as plaintext argument
        os.environ['ORG_GRADLE_PROJECT_signingPassword'] = gpg_password
    else:
      print("  Signing method is gpg tool")
      cmd += ' -PuseGpg -Psigning.gnupg.keyName="%s"' % gpg_key_id
      if gpg_home is not None:
        cmd += ' -Psigning.gnupg.homeDir="%s"' % gpg_home

  print("  Running: %s" % cmd)
  if gpg_password is not None:
    runAndSendGPGPassword(cmd, gpg_password)
  else:
    run(cmd)

  print('  done!')
  print()
  return rev

reVersion1 = re.compile(r'\>(\d+)\.(\d+)\.(\d+)(-alpha|-beta)?/\<', re.IGNORECASE)
reVersion2 = re.compile(r'-(\d+)\.(\d+)\.(\d+)(-alpha|-beta)?\.zip<', re.IGNORECASE)
reDoapRevision = re.compile(r'(\d+)\.(\d+)(?:\.(\d+))?(-alpha|-beta)?', re.IGNORECASE)
def checkDOAPfiles(version):
  # In Lucene DOAP file, verify presence of all releases less than the one being produced.
  errorMessages = []
  for product in ['lucene']:
    url = 'https://archive.apache.org/dist/lucene/%s' % ('java' if product == 'lucene' else product)
    distpage = load(url)
    releases = set()
    for regex in reVersion1, reVersion2:
      for tup in regex.findall(distpage):
        if tup[0] in ('1', '2'):                    # Ignore 1.X and 2.X releases
          continue
        releases.add(normalizeVersion(tup))
    doapNS = '{http://usefulinc.com/ns/doap#}'
    xpathRevision = '{0}Project/{0}release/{0}Version/{0}revision'.format(doapNS)
    doapFile = "dev-tools/doap/%s.rdf" % product
    treeRoot = ET.parse(doapFile).getroot()
    doapRevisions = set()
    for revision in treeRoot.findall(xpathRevision):
      match = reDoapRevision.match(revision.text)
      if (match is not None):
        if (match.group(1) not in ('0', '1', '2')): # Ignore 0.X, 1.X and 2.X revisions
          doapRevisions.add(normalizeVersion(match.groups()))
      else:
        errorMessages.append('ERROR: Failed to parse revision: %s in %s' % (revision.text, doapFile))
    missingDoapRevisions = set()
    for release in releases:
      if release not in doapRevisions and release < version: # Ignore releases greater than the one being produced
        missingDoapRevisions.add(release)
    if len(missingDoapRevisions) > 0:
      errorMessages.append('ERROR: Missing revision(s) in %s: %s' % (doapFile, ', '.join(sorted(missingDoapRevisions))))
  if (len(errorMessages) > 0):
    raise RuntimeError('\n%s\n(Hint: copy/paste from the stable branch version of the file(s).)'
                       % '\n'.join(errorMessages))

def normalizeVersion(tup):
  suffix = ''
  if tup[-1] is not None and tup[-1].lower() == '-alpha':
    tup = tup[:(len(tup) - 1)]
    suffix = '-ALPHA'
  elif tup[-1] is not None and tup[-1].lower() == '-beta':
    tup = tup[:(len(tup) - 1)]
    suffix = '-BETA'
  while tup[-1] in ('', None):
    tup = tup[:(len(tup) - 1)]
  while len(tup) < 3:
    tup = tup + ('0',)
  return '.'.join(tup) + suffix


def pushLocal(version, root, rcNum, localDir):
  print('Push local [%s]...' % localDir)
  os.makedirs(localDir)

  lucene_dist_dir = '%s/lucene/distribution/build/release' % root
  rev = open('%s/lucene/distribution/build/release/.gitrev' % root, encoding='UTF-8').read()

  dir = 'lucene-%s-RC%d-rev-%s' % (version, rcNum, rev)
  os.makedirs('%s/%s/lucene' % (localDir, dir))
  print('  Lucene')
  os.chdir(lucene_dist_dir)
  print('    archive...')
  if os.path.exists('lucene.tar'):
    os.remove('lucene.tar')
  run('tar cf lucene.tar *')

  os.chdir('%s/%s/lucene' % (localDir, dir))
  print('    extract...')
  run('tar xf "%s/lucene.tar"' % lucene_dist_dir)
  os.remove('%s/lucene.tar' % lucene_dist_dir)
  os.chdir('..')

  print('  chmod...')
  run('chmod -R a+rX-w .')

  print('  done!')
  return 'file://%s/%s' % (os.path.abspath(localDir), dir)


def read_version(path):
  return scriptutil.find_current_version()


def parse_config():
  epilogue = textwrap.dedent('''
    Example usage for a Release Manager:
    python3 -u dev-tools/scripts/buildAndPushRelease.py --push-local /tmp/releases/6.0.1 --sign 6E68DA61 --rc-num 1
  ''')
  description = 'Utility to build, push, and test a release.'
  parser = argparse.ArgumentParser(description=description, epilog=epilogue,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--no-prepare', dest='prepare', default=True, action='store_false',
                      help='Use the already built release in the provided checkout')
  parser.add_argument('--local-keys', metavar='PATH',
                      help='Uses local KEYS file to validate presence of RM\'s gpg key')
  parser.add_argument('--push-local', metavar='PATH',
                      help='Push the release to the local path')
  parser.add_argument('--sign', metavar='KEYID',
                      help='Sign the release with the given gpg key')
  parser.add_argument('--sign-method-gradle', dest='sign_method_gradle', default=False, action='store_true',
                      help='Use Gradle built-in GPG signing instead of gpg command for signing artifacts. '
                      ' This may require --gpg-secring argument if your keychain cannot be resolved automatically.')
  parser.add_argument('--gpg-pass-noprompt', dest='gpg_pass_noprompt', default=False, action='store_true',
                      help='Do not prompt for gpg passphrase. For the default gnupg method, this means your gpg-agent'
                      ' needs a non-TTY pin-entry program. For gradle signing method, passphrase must be provided'
                      ' in gradle.properties or by env.var/sysprop. See ./gradlew helpPublishing for more info')
  parser.add_argument('--gpg-home', metavar='PATH',
                      help='Path to gpg home containing your secring.gpg'
                      ' Optional, will use $HOME/.gnupg/secring.gpg by default')
  parser.add_argument('--rc-num', metavar='NUM', type=int, default=1,
                      help='Release Candidate number.  Default: 1')
  parser.add_argument('--root', metavar='PATH', default='.',
                      help='Root of Git working tree for lucene.  Default: "." (the current directory)')
  parser.add_argument('--logfile', metavar='PATH',
                      help='Specify log file path (default /tmp/release.log)')
  parser.add_argument('--dev-mode', default=False, action='store_true',
                      help='Enable development mode, which disables some strict checks')
  config = parser.parse_args()

  if not config.prepare and config.sign:
    parser.error('Cannot sign already built release')
  if config.push_local is not None and os.path.exists(config.push_local):
    parser.error('Cannot push to local path that already exists')
  if config.rc_num <= 0:
    parser.error('Release Candidate number must be a positive integer')
  if not os.path.isdir(config.root):
    parser.error('Root path "%s" is not a directory' % config.root)
  if config.local_keys is not None and not os.path.exists(config.local_keys):
    parser.error('Local KEYS file "%s" not found' % config.local_keys)
  if config.gpg_home and not os.path.exists(os.path.join(config.gpg_home, 'secring.gpg')):
    parser.error('Specified gpg home %s does not exist or does not contain a secring.gpg' % config.gpg_home)
  global dev_mode
  if config.dev_mode:
    print("Enabling development mode - DO NOT USE FOR ACTUAL RELEASE!")
    dev_mode = True
  cwd = os.getcwd()
  os.chdir(config.root)
  config.root = os.getcwd() # Absolutize root dir
  if os.system('git rev-parse') or 2 != len([d for d in ('dev-tools','lucene') if os.path.isdir(d)]):
    parser.error('Root path "%s" is not a valid lucene checkout' % config.root)
  os.chdir(cwd)
  global LOG
  if config.logfile:
    LOG = config.logfile
  print("Logfile is: %s" % LOG)

  config.version = read_version(config.root)
  print('Building version: %s' % config.version)

  return config

def check_cmdline_tools():  # Fail fast if there are cmdline tool problems
  if os.system('git --version >/dev/null 2>/dev/null'):
    raise RuntimeError('"git --version" returned a non-zero exit code.')

def check_key_in_keys(gpgKeyID, local_keys):
  if gpgKeyID is not None:
    print('  Verify your gpg key is in the main KEYS file')
    if local_keys is not None:
      print("    Using local KEYS file %s" % local_keys)
      keysFileText = open(local_keys, encoding='iso-8859-1').read()
      keysFileLocation = local_keys
    else:
      keysFileURL = "https://archive.apache.org/dist/lucene/KEYS"
      keysFileLocation = keysFileURL
      print("    Using online KEYS file %s" % keysFileURL)
      keysFileText = load(keysFileURL, encoding='iso-8859-1')
    if len(gpgKeyID) > 2 and gpgKeyID[0:2] == '0x':
      gpgKeyID = gpgKeyID[2:]
    if len(gpgKeyID) > 40:
      gpgKeyID = gpgKeyID.replace(" ", "")
    if len(gpgKeyID) == 8:
      gpgKeyID8Char = "%s %s" % (gpgKeyID[0:4], gpgKeyID[4:8])
      re_to_match = r"^pub .*\n\s+(\w{4} \w{4} \w{4} \w{4} \w{4}  \w{4} \w{4} \w{4} %s|\w{32}%s)" % (gpgKeyID8Char, gpgKeyID)
    elif len(gpgKeyID) == 40:
      gpgKeyID40Char = "%s %s %s %s %s  %s %s %s %s %s" % \
                       (gpgKeyID[0:4], gpgKeyID[4:8], gpgKeyID[8:12], gpgKeyID[12:16], gpgKeyID[16:20],
                       gpgKeyID[20:24], gpgKeyID[24:28], gpgKeyID[28:32], gpgKeyID[32:36], gpgKeyID[36:])
      re_to_match = r"^pub .*\n\s+(%s|%s)" % (gpgKeyID40Char, gpgKeyID)
    else:
      print('Invalid gpg key id format [%s]. Must be 8 byte short ID or 40 byte fingerprint, with or without 0x prefix, no spaces.' % gpgKeyID)
      exit(2)
    if re.search(re_to_match, keysFileText, re.MULTILINE):
      print('    Found key %s in KEYS file at %s' % (gpgKeyID, keysFileLocation))
    else:
      print('    ERROR: Did not find your key %s in KEYS file at %s. Please add it and try again.' % (gpgKeyID, keysFileLocation))
      if local_keys is not None:
        print('           You are using a local KEYS file. Make sure it is up to date or validate against the online version')
      exit(2)


def resolve_gpghome():
  for p in [
    # Linux, macos
    os.path.join(os.path.expanduser("~"), '.gnupg'),
    # Windows 10
    os.path.expandvars(r'%APPDATA%\GnuPG')
    # TODO: Should we support Cygwin?
  ]:
    if os.path.exists(os.path.join(p, 'secring.gpg')):
      return p
  return None


def main():
  check_cmdline_tools()

  c = parse_config()
  gpg_home = None

  if c.sign:
    sys.stdout.flush()
    c.key_id = c.sign
    check_key_in_keys(c.key_id, c.local_keys)
    if c.gpg_home is not None:
      print("Using custom gpg-home: %s" % c.gpg_home)
      gpg_home = c.gpg_home
    if c.sign_method_gradle:
      if gpg_home is None:
        resolved_gpg_home = resolve_gpghome()
        if resolved_gpg_home is not None:
          print("Resolved gpg home to %s" % resolved_gpg_home)
          gpg_home = resolved_gpg_home
        else:
          print("WARN: Could not locate your gpg secret keyring, and --gpg-home not specified.")
          print("      Falling back to location configured in gradle.properties.")
          print("      See 'gradlew helpPublishing' for details.")
          gpg_home = None
    if c.gpg_pass_noprompt:
      print("Will not prompt for gpg password. Make sure your signing setup supports this.")
      c.key_password = None
    else:
      import getpass
      c.key_password = getpass.getpass('Enter GPG keystore password: ')
  else:
    c.key_id = None
    c.key_password = None

  if c.prepare:
    prepare(c.root, c.version, c.key_id, c.key_password, gpg_home=gpg_home, sign_gradle=c.sign_method_gradle)
  else:
    os.chdir(c.root)

  if c.push_local:
    url = pushLocal(c.version, c.root, c.rc_num, c.push_local)
  else:
    url = None

  if url is not None:
    print('  URL: %s' % url)
    print('Next run the smoker tester:')
    p = re.compile(".*/")
    m = p.match(sys.argv[0])
    if not c.sign:
      signed = "--not-signed"
    else:
      signed = ""
    print('%s -u %ssmokeTestRelease.py %s %s' % (sys.executable, m.group(), signed, url))

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Keyboard interrupt...exiting')

