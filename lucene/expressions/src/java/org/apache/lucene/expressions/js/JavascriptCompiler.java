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
package org.apache.lucene.expressions.js;

import java.io.IOException;
import java.io.Reader;
import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassFile.ClassHierarchyResolverOption;
import java.lang.classfile.ClassHierarchyResolver;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.CodeBuilder.BlockCodeBuilder;
import java.lang.classfile.Label;
import java.lang.classfile.Opcode;
import java.lang.classfile.TypeKind;
import java.lang.classfile.attribute.ExceptionsAttribute;
import java.lang.classfile.instruction.BranchInstruction;
import java.lang.classfile.instruction.OperatorInstruction;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.DynamicConstantDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptParser.ExpressionContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;

/**
 * An expression compiler for javascript expressions.
 *
 * <p>Example:
 *
 * <pre class="prettyprint">
 *   Expression foo = JavascriptCompiler.compile("((0.3*popularity)/10.0)+(0.7*score)");
 * </pre>
 *
 * <p>See the {@link org.apache.lucene.expressions.js package documentation} for the supported
 * syntax and default functions.
 *
 * <p>You can compile with an alternate set of functions via {@link #compile(String, Map)}. For
 * example:
 *
 * <pre class="prettyprint">
 *   Map&lt;String,MethodHandle&gt; functions = new HashMap&lt;&gt;();
 *   // add all the default functions
 *   functions.putAll(JavascriptCompiler.DEFAULT_FUNCTIONS);
 *   // add cbrt()
 *   functions.put("cbrt", MethodHandles.publicLookup().findStatic(Math.class, "cbrt",
 *                 MethodType.methodType(double.class, double.class)));
 *   // call compile with customized function map
 *   Expression foo = JavascriptCompiler.compile("cbrt(score)+ln(popularity)",
 *                                               functions);
 * </pre>
 *
 * <p>It is possible to pass any {@link MethodHandle} as function that only takes {@code double}
 * parameters and returns a {@code double}. The method does not need to be public, it just needs to
 * be resolved correctly using a private {@link Lookup} instance. Ideally the methods should be
 * {@code static}, but you can use {@link MethodHandle#bindTo(Object)} to bind it to a receiver.
 *
 * @lucene.experimental
 */
public final class JavascriptCompiler {

  private static final Lookup LOOKUP = MethodHandles.lookup();

  private static final MethodType MT_EXPRESSION_CTOR_LOOKUP =
      MethodType.methodType(void.class, String.class, String[].class);

  // We use the same class name for all generated classes (they are hidden anyways).
  // The source code is displayed as "source file name" in stack trace.
  private static final ClassDesc
      CD_CompiledExpression =
          ClassDesc.of(JavascriptCompiler.class.getName() + "$CompiledExpression"),
      CD_Expression = Expression.class.describeConstable().orElseThrow(),
      CD_DoubleValues = DoubleValues.class.describeConstable().orElseThrow(),
      CD_JavascriptCompiler = JavascriptCompiler.class.describeConstable().orElseThrow();
  private static final MethodTypeDesc
      MTD_EXPRESSION_CTOR =
          MethodTypeDesc.of(
              ConstantDescs.CD_void, ConstantDescs.CD_String, ConstantDescs.CD_String.arrayType()),
      MTD_EVALUATE = MethodTypeDesc.of(ConstantDescs.CD_double, CD_DoubleValues.arrayType()),
      MTD_DOUBLE_VAL = MethodTypeDesc.of(ConstantDescs.CD_double),
      MTD_PATCH_STACK =
          MethodTypeDesc.of(ConstantDescs.CD_Throwable, ConstantDescs.CD_Throwable, CD_Expression);

  private static final ExceptionsAttribute ATTR_THROWS_IOEXCEPTION =
      ExceptionsAttribute.ofSymbols(IOException.class.describeConstable().orElseThrow());

  final String sourceText;
  final Map<String, MethodHandle> functions;
  final boolean picky;

  /**
   * Compiles the given expression using default compiler settings.
   *
   * @param sourceText The expression to compile
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  public static Expression compile(String sourceText) throws ParseException {
    return compile(sourceText, DEFAULT_FUNCTIONS);
  }

  /**
   * Compiles the given expression with the supplied custom functions using default compiler
   * settings.
   *
   * <p>Functions must be {@code public static}, return {@code double} and can take from zero to 256
   * {@code double} parameters.
   *
   * @param sourceText The expression to compile
   * @param functions map of String names to {@link MethodHandle}s
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  public static Expression compile(String sourceText, Map<String, MethodHandle> functions)
      throws ParseException {
    return compile(sourceText, functions, false);
  }

  /**
   * Compiles the given expression with the supplied custom functions.
   *
   * <p>Functions must be {@code public static}, return {@code double} and can take from zero to 256
   * {@code double} parameters.
   *
   * @param sourceText The expression to compile
   * @param functions map of String names to {@link MethodHandle}s
   * @param picky whether to throw exception on ambiguity or other internal parsing issues (this
   *     option makes things slower too, it is only for debugging).
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  static Expression compile(String sourceText, Map<String, MethodHandle> functions, boolean picky)
      throws ParseException {
    for (MethodHandle m : functions.values()) {
      checkFunction(m);
    }
    return new JavascriptCompiler(sourceText, functions, picky).compileExpression();
  }

  /**
   * Constructs a compiler for expressions with specific set of functions
   *
   * @param sourceText The expression to compile
   */
  private JavascriptCompiler(
      String sourceText, Map<String, MethodHandle> functions, boolean picky) {
    this.sourceText = Objects.requireNonNull(sourceText, "sourceText");
    this.functions = Map.copyOf(functions);
    this.picky = picky;
  }

  /**
   * Compiles the given expression as hidden class.
   *
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  @SuppressForbidden(reason = "defines new bytecode on purpose, carefully")
  private Expression compileExpression() throws ParseException {
    final Map<String, Integer> externalsMap = new LinkedHashMap<>(),
        constantsMap = new LinkedHashMap<>();
    final byte[] classFile;
    try {
      classFile =
          ClassFile.of(
                  ClassHierarchyResolverOption.of(ClassHierarchyResolver.ofClassLoading(LOOKUP)))
              .build(
                  CD_CompiledExpression,
                  writer -> generateClass(getAntlrParseTree(), writer, externalsMap, constantsMap));
    } catch (RuntimeException re) {
      // Unwrap the ParseException from RuntimeException
      if (re.getCause() instanceof ParseException cause) {
        throw cause;
      }
      throw re;
    }

    try {
      final Lookup lookup =
          LOOKUP.defineHiddenClassWithClassData(
              classFile, constantsMap.keySet().stream().map(functions::get).toList(), true);
      return invokeConstructor(lookup, lookup.lookupClass(), externalsMap);
    } catch (ReflectiveOperationException exception) {
      throw new IllegalStateException(
          "An internal error occurred attempting to compile the expression (" + sourceText + ").",
          exception);
    }
  }

  private Expression invokeConstructor(
      Lookup lookup, Class<?> expressionClass, Map<String, Integer> externalsMap)
      throws ReflectiveOperationException {
    final MethodHandle ctor = lookup.findConstructor(expressionClass, MT_EXPRESSION_CTOR_LOOKUP);
    try {
      return (Expression) ctor.invoke(sourceText, externalsMap.keySet().toArray(String[]::new));
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  /**
   * Parses the sourceText into an ANTLR 4 parse tree
   *
   * @return The ANTLR parse tree
   */
  private ParseTree getAntlrParseTree() {
    final JavascriptErrorHandlingLexer javascriptLexer =
        new JavascriptErrorHandlingLexer(CharStreams.fromString(sourceText));
    javascriptLexer.removeErrorListeners();
    final JavascriptParser javascriptParser =
        new JavascriptParser(new CommonTokenStream(javascriptLexer));
    javascriptParser.removeErrorListeners();
    if (picky) {
      setupPicky(javascriptParser);
    }
    javascriptParser.setErrorHandler(new JavascriptParserErrorStrategy());
    return javascriptParser.compile();
  }

  private void setupPicky(JavascriptParser parser) {
    // Diagnostic listener invokes syntaxError on other listeners for ambiguity issues
    parser.addErrorListener(new DiagnosticErrorListener(true));
    // a second listener to fail the test when the above happens.
    parser.addErrorListener(
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              final Recognizer<?, ?> recognizer,
              final Object offendingSymbol,
              final int line,
              final int charPositionInLine,
              final String msg,
              final RecognitionException e) {
            throw newWrappedParseException(
                "line ("
                    + line
                    + "), offset ("
                    + charPositionInLine
                    + "), symbol ("
                    + offendingSymbol
                    + ") "
                    + msg,
                charPositionInLine);
          }
        });

    // Enable exact ambiguity detection (costly). we enable exact since its the default for
    // DiagnosticErrorListener, life is too short to think about what 'inexact ambiguity' might
    // mean.
    parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
  }

  /** Sends the bytecode of class file to {@link ClassBuilder}. */
  private void generateClass(
      final ParseTree parseTree,
      final ClassBuilder builder,
      final Map<String, Integer> externalsMap,
      final Map<String, Integer> constantsMap) {
    builder.withFlags(ClassFile.ACC_PUBLIC | ClassFile.ACC_FINAL).withSuperclass(CD_Expression);

    // constructor:
    builder.withMethodBody(
        ConstantDescs.INIT_NAME,
        MTD_EXPRESSION_CTOR,
        ClassFile.ACC_PUBLIC,
        gen -> {
          gen.aload(0); // this
          gen.aload(gen.parameterSlot(0));
          gen.aload(gen.parameterSlot(1));
          gen.invokespecial(CD_Expression, ConstantDescs.INIT_NAME, MTD_EXPRESSION_CTOR);
          gen.return_();
        });

    // evaluate method:
    final Consumer<BlockCodeBuilder> mainBlock =
        gen -> {
          newClassFileGeneratorVisitor(gen, externalsMap, constantsMap).visit(parseTree);
          gen.dreturn();
        };
    final Consumer<BlockCodeBuilder> rethrowBlock =
        gen -> {
          gen.aload(0); // this
          gen.invokestatic(CD_JavascriptCompiler, "patchStackTrace", MTD_PATCH_STACK);
          gen.athrow();
        };
    final Consumer<CodeBuilder> body =
        gen -> gen.trying(mainBlock, c -> c.catching(ConstantDescs.CD_Throwable, rethrowBlock));
    builder.withMethod(
        "evaluate",
        MTD_EVALUATE,
        ClassFile.ACC_PUBLIC,
        mth -> mth.with(ATTR_THROWS_IOEXCEPTION).withCode(body));
  }

  private JavascriptBaseVisitor<Void> newClassFileGeneratorVisitor(
      CodeBuilder gen,
      final Map<String, Integer> externalsMap,
      final Map<String, Integer> constantsMap) {
    // to completely hide the ANTLR visitor we use an anonymous impl:
    return new JavascriptBaseVisitor<Void>() {
      private final Deque<TypeKind> typeStack = new ArrayDeque<>();

      @Override
      public Void visitCompile(JavascriptParser.CompileContext ctx) {
        typeStack.push(TypeKind.DOUBLE);
        visit(ctx.expression());
        typeStack.pop();

        return null;
      }

      @Override
      public Void visitPrecedence(JavascriptParser.PrecedenceContext ctx) {
        visit(ctx.expression());

        return null;
      }

      @Override
      public Void visitNumeric(JavascriptParser.NumericContext ctx) {
        if (ctx.HEX() != null) {
          pushLong(Long.parseLong(ctx.HEX().getText().substring(2), 16));
        } else if (ctx.OCTAL() != null) {
          pushLong(Long.parseLong(ctx.OCTAL().getText().substring(1), 8));
        } else if (ctx.DECIMAL() != null) {
          gen.loadConstant(Double.parseDouble(ctx.DECIMAL().getText()));
          gen.conversion(TypeKind.DOUBLE, typeStack.peek());
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        return null;
      }

      @Override
      public Void visitExternal(JavascriptParser.ExternalContext ctx) {
        String text = ctx.VARIABLE().getText();
        int arguments = ctx.expression().size();
        boolean parens = ctx.LP() != null && ctx.RP() != null;
        MethodHandle mh = parens ? functions.get(text) : null;

        if (mh != null) {
          final int arity = mh.type().parameterCount();

          if (arguments != arity) {
            throw newWrappedParseException(
                "Invalid expression '"
                    + sourceText
                    + "': Expected ("
                    + arity
                    + ") arguments for function call ("
                    + text
                    + "), but found ("
                    + arguments
                    + ").",
                ctx.start.getStartIndex());
          }

          // place dynamic constant with MethodHandle on top of stack
          final int index = constantsMap.computeIfAbsent(text, _ -> constantsMap.size());
          final var constantDesc =
              DynamicConstantDesc.ofNamed(
                  ConstantDescs.BSM_CLASS_DATA_AT,
                  ConstantDescs.DEFAULT_NAME,
                  ConstantDescs.CD_MethodHandle,
                  index);
          gen.loadConstant(constantDesc);

          // add arguments:
          typeStack.push(TypeKind.DOUBLE);
          for (int argument = 0; argument < arguments; ++argument) {
            visit(ctx.expression(argument));
          }
          typeStack.pop();

          // invoke MethodHandle of function:
          gen.invokevirtual(
              ConstantDescs.CD_MethodHandle,
              "invokeExact",
              mh.type().describeConstable().orElseThrow());

          gen.conversion(TypeKind.DOUBLE, typeStack.peek());
        } else if (!parens || arguments == 0 && text.contains(".")) {
          text = normalizeQuotes(ctx.getText());
          final int index = externalsMap.computeIfAbsent(text, _ -> externalsMap.size());

          gen.aload(gen.parameterSlot(0));
          gen.loadConstant(index);
          gen.arrayLoad(TypeKind.REFERENCE);
          gen.invokevirtual(CD_DoubleValues, "doubleValue", MTD_DOUBLE_VAL);
          gen.conversion(TypeKind.DOUBLE, typeStack.peek());
        } else {
          throw newWrappedParseException(
              "Invalid expression '" + sourceText + "': Unrecognized function call (" + text + ").",
              ctx.start.getStartIndex());
        }
        return null;
      }

      @Override
      public Void visitUnary(JavascriptParser.UnaryContext ctx) {
        if (ctx.BOOLNOT() != null) {
          Label labelNotTrue = gen.newLabel();
          Label labelNotReturn = gen.newLabel();

          typeStack.push(TypeKind.INT);
          visit(ctx.expression());
          typeStack.pop();
          gen.ifeq(labelNotTrue);
          pushBoolean(false);
          gen.goto_(labelNotReturn);
          gen.labelBinding(labelNotTrue);
          pushBoolean(true);
          gen.labelBinding(labelNotReturn);

        } else if (ctx.BWNOT() != null) {
          typeStack.push(TypeKind.LONG);
          visit(ctx.expression());
          typeStack.pop();
          gen.loadConstant(-1L);
          gen.lxor();
          gen.conversion(TypeKind.LONG, typeStack.peek());

        } else if (ctx.ADD() != null) {
          visit(ctx.expression());

        } else if (ctx.SUB() != null) {
          typeStack.push(TypeKind.DOUBLE);
          visit(ctx.expression());
          typeStack.pop();
          gen.dneg();
          gen.conversion(TypeKind.DOUBLE, typeStack.peek());

        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        return null;
      }

      @Override
      public Void visitMuldiv(JavascriptParser.MuldivContext ctx) {
        Opcode opcode;

        if (ctx.MUL() != null) {
          opcode = Opcode.DMUL;
        } else if (ctx.DIV() != null) {
          opcode = Opcode.DDIV;
        } else if (ctx.REM() != null) {
          opcode = Opcode.DREM;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushArith(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitAddsub(JavascriptParser.AddsubContext ctx) {
        Opcode opcode;

        if (ctx.ADD() != null) {
          opcode = Opcode.DADD;
        } else if (ctx.SUB() != null) {
          opcode = Opcode.DSUB;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushArith(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwshift(JavascriptParser.BwshiftContext ctx) {
        Opcode opcode;

        if (ctx.LSH() != null) {
          opcode = Opcode.LSHL;
        } else if (ctx.RSH() != null) {
          opcode = Opcode.LSHR;
        } else if (ctx.USH() != null) {
          opcode = Opcode.LUSHR;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushShift(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBoolcomp(JavascriptParser.BoolcompContext ctx) {
        Opcode opcode;

        if (ctx.LT() != null) {
          opcode = Opcode.IFLT;
        } else if (ctx.LTE() != null) {
          opcode = Opcode.IFLE;
        } else if (ctx.GT() != null) {
          opcode = Opcode.IFGT;
        } else if (ctx.GTE() != null) {
          opcode = Opcode.IFGE;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushCond(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBooleqne(JavascriptParser.BooleqneContext ctx) {
        Opcode opcode;

        if (ctx.EQ() != null) {
          opcode = Opcode.IFEQ;
        } else if (ctx.NE() != null) {
          opcode = Opcode.IFNE;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushCond(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      private void pushCond(Opcode operator, ExpressionContext left, ExpressionContext right) {
        Label labelTrue = gen.newLabel();
        Label labelReturn = gen.newLabel();

        typeStack.push(TypeKind.DOUBLE);
        visit(left);
        visit(right);
        typeStack.pop();

        if (operator == Opcode.IFGE || operator == Opcode.IFGT) {
          gen.dcmpl();
        } else {
          gen.dcmpg();
        }
        gen.with(BranchInstruction.of(operator, labelTrue));
        pushBoolean(false);
        gen.goto_(labelReturn);
        gen.labelBinding(labelTrue);
        pushBoolean(true);
        gen.labelBinding(labelReturn);
      }

      @Override
      public Void visitBwand(JavascriptParser.BwandContext ctx) {
        pushBitwise(Opcode.LAND, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwxor(JavascriptParser.BwxorContext ctx) {
        pushBitwise(Opcode.LXOR, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwor(JavascriptParser.BworContext ctx) {
        pushBitwise(Opcode.LOR, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBooland(JavascriptParser.BoolandContext ctx) {
        Label andFalse = gen.newLabel();
        Label andEnd = gen.newLabel();

        typeStack.push(TypeKind.INT);
        visit(ctx.expression(0));
        gen.ifeq(andFalse);
        visit(ctx.expression(1));
        gen.ifeq(andFalse);
        typeStack.pop();
        pushBoolean(true);
        gen.goto_(andEnd);
        gen.labelBinding(andFalse);
        pushBoolean(false);
        gen.labelBinding(andEnd);

        return null;
      }

      @Override
      public Void visitBoolor(JavascriptParser.BoolorContext ctx) {
        Label orTrue = gen.newLabel();
        Label orEnd = gen.newLabel();

        typeStack.push(TypeKind.INT);
        visit(ctx.expression(0));
        gen.ifne(orTrue);
        visit(ctx.expression(1));
        gen.ifne(orTrue);
        typeStack.pop();
        pushBoolean(false);
        gen.goto_(orEnd);
        gen.labelBinding(orTrue);
        pushBoolean(true);
        gen.labelBinding(orEnd);

        return null;
      }

      @Override
      public Void visitConditional(JavascriptParser.ConditionalContext ctx) {
        Label condFalse = gen.newLabel();
        Label condEnd = gen.newLabel();

        typeStack.push(TypeKind.INT);
        visit(ctx.expression(0));
        typeStack.pop();
        gen.ifeq(condFalse);
        visit(ctx.expression(1));
        gen.goto_(condEnd);
        gen.labelBinding(condFalse);
        visit(ctx.expression(2));
        gen.labelBinding(condEnd);

        return null;
      }

      private void pushArith(Opcode operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, TypeKind.DOUBLE, TypeKind.DOUBLE, TypeKind.DOUBLE);
      }

      private void pushShift(Opcode operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, TypeKind.LONG, TypeKind.INT, TypeKind.LONG);
      }

      private void pushBitwise(Opcode operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, TypeKind.LONG, TypeKind.LONG, TypeKind.LONG);
      }

      private void pushBinaryOp(
          Opcode operator,
          ExpressionContext left,
          ExpressionContext right,
          TypeKind leftType,
          TypeKind rightType,
          TypeKind returnType) {
        typeStack.push(leftType);
        visit(left);
        typeStack.pop();
        typeStack.push(rightType);
        visit(right);
        typeStack.pop();
        gen.with(OperatorInstruction.of(operator));
        gen.conversion(returnType, typeStack.peek());
      }

      private void pushBoolean(boolean truth) {
        switch (typeStack.peek()) {
          case TypeKind.INT:
            gen.loadConstant(truth ? 1 : 0);
            break;
          case TypeKind.LONG:
            gen.loadConstant(truth ? 1L : 0L);
            break;
          case TypeKind.DOUBLE:
            gen.loadConstant(truth ? 1. : 0.);
            break;
          // $CASES-OMITTED$
          default:
            throw new IllegalStateException("Invalid expected type: " + typeStack.peek());
        }
      }

      private void pushLong(long i) {
        switch (typeStack.peek()) {
          case TypeKind.INT:
            gen.loadConstant((int) i);
            break;
          case TypeKind.LONG:
            gen.loadConstant(i);
            break;
          case TypeKind.DOUBLE:
            gen.loadConstant((double) i);
            break;
          // $CASES-OMITTED$
          default:
            throw new IllegalStateException("Invalid expected type: " + typeStack.peek());
        }
      }
    };
  }

  static String normalizeQuotes(String text) {
    StringBuilder out = new StringBuilder(text.length());
    boolean inDoubleQuotes = false;
    for (int i = 0; i < text.length(); ++i) {
      char c = text.charAt(i);
      if (c == '\\') {
        c = text.charAt(++i);
        if (c == '\\') {
          out.append('\\'); // re-escape the backslash
        }
        // no escape for double quote
      } else if (c == '\'') {
        if (inDoubleQuotes) {
          // escape in output
          out.append('\\');
        } else {
          int j = findSingleQuoteStringEnd(text, i);
          out.append(text, i, j); // copy up to end quote (leave end for append below)
          i = j;
        }
      } else if (c == '"') {
        c = '\''; // change beginning/ending doubles to singles
        inDoubleQuotes = !inDoubleQuotes;
      }
      out.append(c);
    }
    return out.toString();
  }

  static int findSingleQuoteStringEnd(String text, int start) {
    ++start; // skip beginning
    while (text.charAt(start) != '\'') {
      if (text.charAt(start) == '\\') {
        ++start; // blindly consume escape value
      }
      ++start;
    }
    return start;
  }

  /**
   * Returns a new {@link ParseException} wrapped in a {@link RuntimeException} because the API
   * doesn't allow checked exceptions, so propagate up the stack. This is unwrapped in {@link
   * #compileExpression()}.
   */
  static RuntimeException newWrappedParseException(String text, int position) {
    return new RuntimeException(new ParseException(text, position));
  }

  /**
   * The default set of functions available to expressions.
   *
   * <p>See the {@link org.apache.lucene.expressions.js package documentation} for a list.
   */
  public static final Map<String, MethodHandle> DEFAULT_FUNCTIONS = loadDefaultFunctions();

  private static Map<String, MethodHandle> loadDefaultFunctions() {
    final Map<String, MethodHandle> map = new HashMap<>();
    final Lookup publicLookup = MethodHandles.publicLookup();
    try {
      final Properties props = new Properties();
      var name = JavascriptCompiler.class.getSimpleName() + ".properties";
      try (Reader in =
          IOUtils.getDecodingReader(
              IOUtils.requireResourceNonNull(
                  JavascriptCompiler.class.getResourceAsStream(name), name),
              StandardCharsets.UTF_8)) {
        props.load(in);
      }
      for (final String call : props.stringPropertyNames()) {
        final String[] vals = props.getProperty(call).split(",");
        if (vals.length != 3) {
          throw new Error("Syntax error while reading Javascript functions from resource");
        }
        final Class<?> clazz = Class.forName(vals[0].trim());
        final String methodName = vals[1].trim();
        final int arity = Integer.parseInt(vals[2].trim());
        final MethodHandle mh =
            publicLookup.findStatic(
                clazz,
                methodName,
                MethodType.methodType(double.class, Collections.nCopies(arity, double.class)));
        checkFunction(mh);
        map.put(call, mh);
      }
    } catch (ReflectiveOperationException | IOException e) {
      throw new Error("Cannot resolve function", e);
    }
    return Collections.unmodifiableMap(map);
  }

  /** Check Method signature for compatibility. */
  private static void checkFunction(MethodHandle method) {
    Supplier<String> methodNameSupplier = method::toString;

    // try to crack the handle and check if it is a static call:
    int refKind;
    try {
      MethodHandleInfo cracked = LOOKUP.revealDirect(method);
      refKind = cracked.getReferenceKind();
      // we have a much better name for the method so display it instead:
      methodNameSupplier =
          () ->
              cracked.getDeclaringClass().getName()
                  + "#"
                  + cracked.getName()
                  + cracked.getMethodType();
    } catch (@SuppressWarnings("unused") IllegalArgumentException | SecurityException iae) {
      // can't check for static, we assume it is static
      // (it does not matter as we call the MethodHandle directly if it is compatible):
      refKind = MethodHandleInfo.REF_invokeStatic;
    }
    if (refKind != MethodHandleInfo.REF_invokeStatic && refKind != MethodHandleInfo.REF_getStatic) {
      throw new IllegalArgumentException(methodNameSupplier.get() + " is not static.");
    }

    // do some checks if the signature is "compatible":
    final MethodType type = method.type();
    for (int arg = 0, arity = type.parameterCount(); arg < arity; arg++) {
      if (type.parameterType(arg) != double.class) {
        throw new IllegalArgumentException(
            methodNameSupplier.get() + " must take only double parameters.");
      }
    }
    if (type.returnType() != double.class) {
      throw new IllegalArgumentException(methodNameSupplier.get() + " does not return a double.");
    }
  }

  /**
   * Method called from try/catch handler in compiled expression. This patches the stack trace and
   * adds back a hidden frame (including the source code of script as filename).
   */
  static Throwable patchStackTrace(Throwable t, Expression impl) {
    var extra = new StackTraceElement(impl.getClass().getName(), "evaluate", impl.sourceText, -1);
    var origStack = t.getStackTrace();
    var myStack = new Throwable().getStackTrace();
    var top = Arrays.stream(origStack).limit(Math.max(0, origStack.length - myStack.length + 1));
    var tail = Arrays.stream(myStack).skip(1);
    t.setStackTrace(
        Stream.of(top, Stream.of(extra), tail)
            .flatMap(Function.identity())
            .toArray(StackTraceElement[]::new));
    return t;
  }
}
