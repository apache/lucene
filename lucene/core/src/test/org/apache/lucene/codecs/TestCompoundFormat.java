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
package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;

/**
 * Unit tests for {@link CompoundFormat} functionality.
 *
 * <p>This test class verifies the compound file decision logic, including:
 *
 * <ul>
 *   <li>Default threshold behavior for different merge policies
 *   <li>Global enable/disable functionality
 *   <li>Maximum segment size limits
 *   <li>Custom threshold configurations
 * </ul>
 */
public class TestCompoundFormat extends LuceneTestCase {

  /** Test instance of CompoundFormat with minimal implementation */
  private CompoundFormat format;

  /**
   * Sets up a test CompoundFormat instance with minimal abstract method implementations. The test
   * format focuses on testing the threshold logic rather than actual I/O operations.
   */
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Create a minimal CompoundFormat implementation for testing threshold logic
    format =
        new CompoundFormat() {
          @Override
          public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo segmentInfo) {
            return null; // Not needed for threshold testing
          }

          @Override
          public void write(Directory dir, SegmentInfo segmentInfo, IOContext context) {
            // No-op implementation for testing
          }
        };
  }

  /**
   * Tests that the default thresholds work correctly for different merge policies.
   *
   * <p>Verifies:
   *
   * <ul>
   *   <li>Default document threshold (65536) for LogDocMergePolicy
   *   <li>Default byte threshold (64MB) for other merge policies
   *   <li>Boundary conditions at threshold limits
   * </ul>
   */
  public void testDefaultThresholds() throws IOException {
    // Enable compound files with no size limit
    format.setShouldUseCompoundFile(true);
    format.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY); // Remove size constraints

    MergePolicy docPolicy = new LogDocMergePolicy();
    MergePolicy bytePolicy = new TieredMergePolicy();

    // Verify default threshold values are as expected
    assertEquals(65536, format.getCfsThresholdDocSize());
    assertEquals(64L * 1024 * 1024, format.getCfsThresholdByteSize());

    // Test LogDocMergePolicy uses document count threshold
    assertTrue("Should use CFS at doc threshold", format.useCompoundFile(65536, docPolicy));
    assertFalse("Should not use CFS above doc threshold", format.useCompoundFile(65537, docPolicy));

    // Test other merge policies use byte size threshold (64MB)
    assertTrue(
        "Should use CFS at byte threshold", format.useCompoundFile(64L * 1024 * 1024, bytePolicy));
    assertFalse(
        "Should not use CFS above byte threshold",
        format.useCompoundFile((64L * 1024 * 1024) + 1, bytePolicy));
  }

  /**
   * Tests that compound files can be globally disabled.
   *
   * <p>When compound files are disabled, no segments should use compound files regardless of their
   * size or the configured thresholds.
   */
  public void testDisabledCompoundFile() throws IOException {
    // Globally disable compound files
    format.setShouldUseCompoundFile(false);
    MergePolicy docPolicy = new LogDocMergePolicy();

    // Verify that CFS is never used when globally disabled
    assertFalse(
        "Should not use CFS when disabled (small segment)", format.useCompoundFile(1, docPolicy));
    assertFalse(
        "Should not use CFS when disabled (at threshold)",
        format.useCompoundFile(65536, docPolicy));
  }

  /**
   * Tests the maximum compound file segment size limit.
   *
   * <p>Segments larger than the configured maximum size should not use compound files, even if they
   * would otherwise be eligible based on the threshold settings.
   */
  public void testMaxCFSSegmentSize() throws IOException {
    format.setShouldUseCompoundFile(true);
    format.setMaxCFSSegmentSizeMB(10); // Set 10MB limit
    MergePolicy bytePolicy = new TieredMergePolicy();

    // Test segments below the maximum size limit
    assertTrue(
        "Should use CFS below max size limit",
        format.useCompoundFile(9L * 1024 * 1024, bytePolicy));

    // Test segments above the maximum size limit
    assertFalse(
        "Should not use CFS above max size limit",
        format.useCompoundFile(11L * 1024 * 1024, bytePolicy));
  }

  /**
   * Tests that custom threshold values can be configured and work correctly.
   *
   * <p>Verifies that both document count and byte size thresholds can be customized and that the
   * boundary conditions work properly with the new values.
   */
  public void testCustomThresholds() throws IOException {
    // Configure custom thresholds
    format.setCfsThresholdDocSize(1000); // Custom doc count threshold
    format.setCfsThresholdByteSize(10 * 1024 * 1024); // Custom 10MB byte threshold

    MergePolicy docPolicy = new LogDocMergePolicy();
    MergePolicy bytePolicy = new TieredMergePolicy();

    // Test custom document count threshold
    assertTrue("Should use CFS at custom doc threshold", format.useCompoundFile(1000, docPolicy));
    assertFalse(
        "Should not use CFS above custom doc threshold", format.useCompoundFile(1001, docPolicy));

    // Test custom byte size threshold
    assertTrue(
        "Should use CFS at custom byte threshold",
        format.useCompoundFile(10 * 1024 * 1024, bytePolicy));
    assertFalse(
        "Should not use CFS above custom byte threshold",
        format.useCompoundFile((10 * 1024 * 1024) + 1, bytePolicy));
  }
}
