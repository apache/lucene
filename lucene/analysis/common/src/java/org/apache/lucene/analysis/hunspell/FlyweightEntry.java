package org.apache.lucene.analysis.hunspell;

import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;

/** A mutable entry object used when enumerating the dictionary internally */
abstract class FlyweightEntry {
  abstract boolean hasTitleCase();

  abstract CharsRef root();

  abstract CharSequence lowerCaseRoot();

  abstract IntsRef forms();
}
