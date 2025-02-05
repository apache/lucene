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
package org.apache.lucene.analysis;

import java.io.Reader;
import org.apache.lucene.util.AttributeFactory;

/**
 * Extension to {@link Analyzer} suitable for Analyzers which wrap other Analyzers.
 *
 * <p>{@link #getWrappedAnalyzer(String)} allows the Analyzer to wrap multiple Analyzers which are
 * selected on a per field basis.
 *
 * <p>{@link #wrapComponents(String, Analyzer.TokenStreamComponents)} allows the
 * TokenStreamComponents of the wrapped Analyzer to then be wrapped (such as adding a new {@link
 * TokenFilter} to form new TokenStreamComponents.
 *
 * <p>{@link #wrapReader(String, Reader)} allows the Reader of the wrapped Analyzer to then be
 * wrapped (such as adding a new {@link CharFilter}.
 *
 * <p><b>Important:</b> If you do not want to wrap the TokenStream using {@link
 * #wrapComponents(String, Analyzer.TokenStreamComponents)} or the Reader using {@link
 * #wrapReader(String, Reader)} and just delegate to other analyzers (like by field name), use
 * {@link DelegatingAnalyzerWrapper} as superclass!
 *
 * @see DelegatingAnalyzerWrapper
 * @since 4.0.0
 */
public abstract class AnalyzerWrapper extends Analyzer {
  /**
   * Creates a new AnalyzerWrapper with the given reuse strategy.
   *
   * <p>If you want to wrap a single delegate Analyzer you can probably reuse its strategy when
   * instantiating this subclass: {@code super(delegate.getReuseStrategy());}.
   *
   * <p>If you choose different analyzers per field, use {@link #PER_FIELD_REUSE_STRATEGY}.
   *
   * @see #getReuseStrategy()
   */
  protected AnalyzerWrapper(ReuseStrategy reuseStrategy) {
    super(
        reuseStrategy instanceof DelegatingAnalyzerWrapper.DelegatingReuseStrategy
            ? reuseStrategy
            : new UnwrappingReuseStrategy(reuseStrategy));
  }

  /**
   * Retrieves the wrapped Analyzer appropriate for analyzing the field with the given name
   *
   * @param fieldName Name of the field which is to be analyzed
   * @return Analyzer for the field with the given name. Assumed to be non-null
   */
  protected abstract Analyzer getWrappedAnalyzer(String fieldName);

  /**
   * Wraps / alters the given TokenStreamComponents, taken from the wrapped Analyzer, to form new
   * components. It is through this method that new TokenFilters can be added by AnalyzerWrappers.
   * By default, the given components are returned.
   *
   * @param fieldName Name of the field which is to be analyzed
   * @param components TokenStreamComponents taken from the wrapped Analyzer
   * @return Wrapped / altered TokenStreamComponents.
   */
  protected TokenStreamComponents wrapComponents(
      String fieldName, TokenStreamComponents components) {
    return components;
  }

  /**
   * Wraps / alters the given TokenStream for normalization purposes, taken from the wrapped
   * Analyzer, to form new components. It is through this method that new TokenFilters can be added
   * by AnalyzerWrappers. By default, the given token stream are returned.
   *
   * @param fieldName Name of the field which is to be analyzed
   * @param in TokenStream taken from the wrapped Analyzer
   * @return Wrapped / altered TokenStreamComponents.
   */
  protected TokenStream wrapTokenStreamForNormalization(String fieldName, TokenStream in) {
    return in;
  }

  /**
   * Wraps / alters the given Reader. Through this method AnalyzerWrappers can implement {@link
   * #initReader(String, Reader)}. By default, the given reader is returned.
   *
   * @param fieldName name of the field which is to be analyzed
   * @param reader the reader to wrap
   * @return the wrapped reader
   */
  protected Reader wrapReader(String fieldName, Reader reader) {
    return reader;
  }

  /**
   * Wraps / alters the given Reader. Through this method AnalyzerWrappers can implement {@link
   * #initReaderForNormalization(String, Reader)}. By default, the given reader is returned.
   *
   * @param fieldName name of the field which is to be analyzed
   * @param reader the reader to wrap
   * @return the wrapped reader
   */
  protected Reader wrapReaderForNormalization(String fieldName, Reader reader) {
    return reader;
  }

  @Override
  protected final TokenStreamComponents createComponents(String fieldName) {
    TokenStreamComponents wrappedComponents =
        getWrappedAnalyzer(fieldName).createComponents(fieldName);
    TokenStreamComponents wrapperComponents = wrapComponents(fieldName, wrappedComponents);
    return new TokenStreamComponentsWrapper(wrapperComponents, wrappedComponents);
  }

  @Override
  protected final TokenStream normalize(String fieldName, TokenStream in) {
    return wrapTokenStreamForNormalization(
        fieldName, getWrappedAnalyzer(fieldName).normalize(fieldName, in));
  }

  @Override
  public int getPositionIncrementGap(String fieldName) {
    return getWrappedAnalyzer(fieldName).getPositionIncrementGap(fieldName);
  }

  @Override
  public int getOffsetGap(String fieldName) {
    return getWrappedAnalyzer(fieldName).getOffsetGap(fieldName);
  }

  @Override
  public final Reader initReader(String fieldName, Reader reader) {
    return getWrappedAnalyzer(fieldName).initReader(fieldName, wrapReader(fieldName, reader));
  }

  @Override
  protected final Reader initReaderForNormalization(String fieldName, Reader reader) {
    return getWrappedAnalyzer(fieldName)
        .initReaderForNormalization(fieldName, wrapReaderForNormalization(fieldName, reader));
  }

  @Override
  protected final AttributeFactory attributeFactory(String fieldName) {
    return getWrappedAnalyzer(fieldName).attributeFactory(fieldName);
  }

  /**
   * A {@link org.apache.lucene.analysis.Analyzer.ReuseStrategy} that checks the wrapped analyzer's
   * strategy for reusability. If the wrapped analyzer's strategy returns null, components need to
   * be re-created.
   */
  public static final class UnwrappingReuseStrategy extends ReuseStrategy {
    private final ReuseStrategy reuseStrategy;

    public UnwrappingReuseStrategy(ReuseStrategy reuseStrategy) {
      this.reuseStrategy = reuseStrategy;
    }

    @Override
    public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
      if (analyzer instanceof AnalyzerWrapper wrapper) {
        final Analyzer wrappedAnalyzer = wrapper.getWrappedAnalyzer(fieldName);
        if (wrappedAnalyzer.getReuseStrategy().getReusableComponents(wrappedAnalyzer, fieldName)
            == null) {
          return null;
        }
      }
      return reuseStrategy.getReusableComponents(analyzer, fieldName);
    }

    @Override
    public void setReusableComponents(
        Analyzer analyzer, String fieldName, TokenStreamComponents components) {
      reuseStrategy.setReusableComponents(analyzer, fieldName, components);

      if (analyzer instanceof AnalyzerWrapper wrapper) {
        assert components instanceof TokenStreamComponentsWrapper;
        final TokenStreamComponentsWrapper wrapperComponents =
            (TokenStreamComponentsWrapper) components;
        final Analyzer wrappedAnalyzer = wrapper.getWrappedAnalyzer(fieldName);
        wrappedAnalyzer
            .getReuseStrategy()
            .setReusableComponents(
                wrappedAnalyzer, fieldName, wrapperComponents.getWrappedComponents());
      }
    }
  }

  /**
   * A {@link Analyzer.TokenStreamComponents} that decorates the wrapper with access to the wrapped
   * components.
   */
  static final class TokenStreamComponentsWrapper extends TokenStreamComponents {
    private final TokenStreamComponents wrapped;

    TokenStreamComponentsWrapper(TokenStreamComponents wrapper, TokenStreamComponents wrapped) {
      super(wrapper.getSource(), wrapper.getTokenStream());
      this.wrapped = wrapped;
    }

    TokenStreamComponents getWrappedComponents() {
      return wrapped;
    }
  }
}
