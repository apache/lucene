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
import java.lang.constant.ConstantDescs;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
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
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

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
 *   functions.put("cbrt", MethodHandles.publicLookup().findStatic("cbrt",
 *                 MethodType.methodType(double.class, double.class)));
 *   // call compile with customized function map
 *   Expression foo = JavascriptCompiler.compile("cbrt(score)+ln(popularity)",
 *                                               functions);
 * </pre>
 *
 * @lucene.experimental
 */
public final class JavascriptCompiler {

  private static final Lookup LOOKUP = MethodHandles.lookup();
  private static final Lookup EXPRESSION_LOOKUP = LOOKUP.in(Expression.class);

  private static final int CLASSFILE_VERSION = Opcodes.V11;

  private static final MethodType MT_EXPRESSION_CTOR_LOOKUP =
      MethodType.methodType(void.class, String.class, String[].class);

  // We use the same class name for all generated classes (they are hidden anyways).
  // The source code is displayed as "source file name" in stack trace.
  private static final String COMPILED_EXPRESSION_CLASS =
      JavascriptCompiler.class.getName() + "$CompiledExpression";
  private static final String COMPILED_EXPRESSION_INTERNAL =
      COMPILED_EXPRESSION_CLASS.replace('.', '/');

  private static final Type EXPRESSION_TYPE = Type.getType(Expression.class),
      FUNCTION_VALUES_TYPE = Type.getType(DoubleValues.class),
      METHOD_HANDLE_TYPE = Type.getType(MethodHandle.class);
  private static final Method
      EXPRESSION_CTOR = getAsmMethod(void.class, "<init>", String.class, String[].class),
      EVALUATE_METHOD = getAsmMethod(double.class, "compiledEvaluate", DoubleValues[].class),
      DYNAMIC_CONSTANT_BOOTSTRAP =
          getAsmMethod(
              MethodHandle.class,
              "dynamicConstantBootstrap",
              Lookup.class,
              String.class,
              Class.class,
              String.class),
      DOUBLE_VAL_METHOD = getAsmMethod(double.class, "doubleValue");

  /** create an ASM Method object from return type, method name, and parameters. */
  private static Method getAsmMethod(Class<?> rtype, String name, Class<?>... ptypes) {
    return new Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
  }

  /**
   * Cache reachability for direct linkage by checking they can be loaded from our {@link
   * ClassLoader} on string lookup. This prevents {@link NoClassDefFoundError}.
   */
  private static final ClassValue<Boolean> REACHABLE_FROM_OUR_CLASSLOADER =
      new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
          try {
            return EXPRESSION_LOOKUP.findClass(type.getName()) == type;
          } catch (
              @SuppressWarnings("unused")
              ReflectiveOperationException e) {
            return false;
          }
        }
      };

  final String sourceText;
  final Map<String, MethodHandle> functions;
  final Map<String, MethodHandleInfo> directFunctions;
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
   * This method is unused, it is just here to make sure that the function signatures don't change.
   * If this method fails to compile, you also have to change the byte code generator to correctly
   * use the FunctionValues class.
   */
  @SuppressWarnings({"unused", "null"})
  private static void unusedTestCompile() throws IOException {
    DoubleValues f = null;
    f.doubleValue();
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

    // calculate all functions which can be invoked directly by cracking MethodHandle:
    final HashMap<String, MethodHandleInfo> map = new HashMap<>();
    for (var e : functions.entrySet()) {
      final MethodHandleInfo info;
      try {
        info = EXPRESSION_LOOKUP.revealDirect(e.getValue());
      } catch (
          @SuppressWarnings("unused")
          IllegalArgumentException iae) {
        continue;
      }
      if (info.getReferenceKind() == MethodHandleInfo.REF_invokeStatic
          && REACHABLE_FROM_OUR_CLASSLOADER.get(info.getDeclaringClass())) {
        map.put(e.getKey(), info);
      }
    }
    this.directFunctions = Collections.unmodifiableMap(map);
  }

  /**
   * Compiles the given expression as hidden class.
   *
   * @return A new compiled expression
   * @throws ParseException on failure to compile
   */
  private Expression compileExpression() throws ParseException {
    final Map<String, Integer> externalsMap = new LinkedHashMap<>();
    final ClassWriter classWriter =
        new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

    try {
      generateClass(getAntlrParseTree(), classWriter, externalsMap);
    } catch (RuntimeException re) {
      if (re.getCause() instanceof ParseException) {
        throw (ParseException) re.getCause();
      }
      throw re;
    }

    try {
      final Lookup lookup =
          LOOKUP.defineHiddenClassWithClassData(classWriter.toByteArray(), functions, true);
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
   * @throws ParseException on failure to parse
   */
  private ParseTree getAntlrParseTree() throws ParseException {
    final ANTLRInputStream antlrInputStream = new ANTLRInputStream(sourceText);
    final JavascriptErrorHandlingLexer javascriptLexer =
        new JavascriptErrorHandlingLexer(antlrInputStream);
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
            throw new RuntimeException(
                new ParseException(
                    "line ("
                        + line
                        + "), offset ("
                        + charPositionInLine
                        + "), symbol ("
                        + offendingSymbol
                        + ") "
                        + msg,
                    charPositionInLine));
          }
        });

    // Enable exact ambiguity detection (costly). we enable exact since its the default for
    // DiagnosticErrorListener, life is too short to think about what 'inexact ambiguity' might
    // mean.
    parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
  }

  /** Sends the bytecode of class file to {@link ClassWriter}. */
  private void generateClass(
      final ParseTree parseTree,
      final ClassWriter classWriter,
      final Map<String, Integer> externalsMap)
      throws ParseException {
    classWriter.visit(
        CLASSFILE_VERSION,
        Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL,
        COMPILED_EXPRESSION_INTERNAL,
        null,
        EXPRESSION_TYPE.getInternalName(),
        null);

    final GeneratorAdapter constructor =
        new GeneratorAdapter(Opcodes.ACC_PUBLIC, EXPRESSION_CTOR, null, null, classWriter);
    constructor.loadThis();
    constructor.loadArgs();
    constructor.invokeConstructor(EXPRESSION_TYPE, EXPRESSION_CTOR);
    constructor.returnValue();
    constructor.endMethod();

    final GeneratorAdapter gen =
        new GeneratorAdapter(Opcodes.ACC_PROTECTED, EVALUATE_METHOD, null, null, classWriter);

    // to completely hide the ANTLR visitor we use an anonymous impl:
    new JavascriptBaseVisitor<Void>() {
      private final Deque<Type> typeStack = new ArrayDeque<>();

      @Override
      public Void visitCompile(JavascriptParser.CompileContext ctx) {
        typeStack.push(Type.DOUBLE_TYPE);
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
          gen.push(Double.parseDouble(ctx.DECIMAL().getText()));
          gen.cast(Type.DOUBLE_TYPE, typeStack.peek());
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

        try {
          if (mh != null) {
            final int arity = mh.type().parameterCount();
            final MethodHandleInfo directCallInfo = directFunctions.get(text);

            if (arguments != arity) {
              throw new ParseException(
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

            if (directCallInfo == null) {
              // place dynamic constant with MethodHandle on top of stack
              gen.visitLdcInsn(
                  new ConstantDynamic(
                      getMethodHandleConstantName(text),
                      METHOD_HANDLE_TYPE.getDescriptor(),
                      new Handle(
                          Opcodes.H_INVOKESTATIC,
                          Type.getInternalName(JavascriptCompiler.class),
                          DYNAMIC_CONSTANT_BOOTSTRAP.getName(),
                          DYNAMIC_CONSTANT_BOOTSTRAP.getDescriptor(),
                          false),
                      text));
            }

            typeStack.push(Type.DOUBLE_TYPE);

            for (int argument = 0; argument < arguments; ++argument) {
              visit(ctx.expression(argument));
            }

            typeStack.pop();

            if (directCallInfo != null) {
              gen.visitMethodInsn(
                  Opcodes.INVOKESTATIC,
                  Type.getInternalName(directCallInfo.getDeclaringClass()),
                  directCallInfo.getName(),
                  directCallInfo.getMethodType().descriptorString(),
                  false);
            } else {
              gen.visitMethodInsn(
                  Opcodes.INVOKEVIRTUAL,
                  METHOD_HANDLE_TYPE.getInternalName(),
                  "invokeExact",
                  mh.type().descriptorString(),
                  false);
            }

            gen.cast(Type.DOUBLE_TYPE, typeStack.peek());
          } else if (!parens || arguments == 0 && text.contains(".")) {
            int index;

            text = normalizeQuotes(ctx.getText());

            if (externalsMap.containsKey(text)) {
              index = externalsMap.get(text);
            } else {
              index = externalsMap.size();
              externalsMap.put(text, index);
            }

            gen.loadArg(0);
            gen.push(index);
            gen.arrayLoad(FUNCTION_VALUES_TYPE);
            gen.invokeVirtual(FUNCTION_VALUES_TYPE, DOUBLE_VAL_METHOD);
            gen.cast(Type.DOUBLE_TYPE, typeStack.peek());
          } else {
            throw new ParseException(
                "Invalid expression '"
                    + sourceText
                    + "': Unrecognized function call ("
                    + text
                    + ").",
                ctx.start.getStartIndex());
          }
          return null;
        } catch (ParseException e) {
          // The API doesn't allow checked exceptions here, so propagate up the stack. This is
          // unwrapped
          // in getAntlrParseTree.
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visitUnary(JavascriptParser.UnaryContext ctx) {
        if (ctx.BOOLNOT() != null) {
          Label labelNotTrue = new Label();
          Label labelNotReturn = new Label();

          typeStack.push(Type.INT_TYPE);
          visit(ctx.expression());
          typeStack.pop();
          gen.visitJumpInsn(Opcodes.IFEQ, labelNotTrue);
          pushBoolean(false);
          gen.goTo(labelNotReturn);
          gen.visitLabel(labelNotTrue);
          pushBoolean(true);
          gen.visitLabel(labelNotReturn);

        } else if (ctx.BWNOT() != null) {
          typeStack.push(Type.LONG_TYPE);
          visit(ctx.expression());
          typeStack.pop();
          gen.push(-1L);
          gen.visitInsn(Opcodes.LXOR);
          gen.cast(Type.LONG_TYPE, typeStack.peek());

        } else if (ctx.ADD() != null) {
          visit(ctx.expression());

        } else if (ctx.SUB() != null) {
          typeStack.push(Type.DOUBLE_TYPE);
          visit(ctx.expression());
          typeStack.pop();
          gen.visitInsn(Opcodes.DNEG);
          gen.cast(Type.DOUBLE_TYPE, typeStack.peek());

        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        return null;
      }

      @Override
      public Void visitMuldiv(JavascriptParser.MuldivContext ctx) {
        int opcode;

        if (ctx.MUL() != null) {
          opcode = Opcodes.DMUL;
        } else if (ctx.DIV() != null) {
          opcode = Opcodes.DDIV;
        } else if (ctx.REM() != null) {
          opcode = Opcodes.DREM;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushArith(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitAddsub(JavascriptParser.AddsubContext ctx) {
        int opcode;

        if (ctx.ADD() != null) {
          opcode = Opcodes.DADD;
        } else if (ctx.SUB() != null) {
          opcode = Opcodes.DSUB;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushArith(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwshift(JavascriptParser.BwshiftContext ctx) {
        int opcode;

        if (ctx.LSH() != null) {
          opcode = Opcodes.LSHL;
        } else if (ctx.RSH() != null) {
          opcode = Opcodes.LSHR;
        } else if (ctx.USH() != null) {
          opcode = Opcodes.LUSHR;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushShift(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBoolcomp(JavascriptParser.BoolcompContext ctx) {
        int opcode;

        if (ctx.LT() != null) {
          opcode = GeneratorAdapter.LT;
        } else if (ctx.LTE() != null) {
          opcode = GeneratorAdapter.LE;
        } else if (ctx.GT() != null) {
          opcode = GeneratorAdapter.GT;
        } else if (ctx.GTE() != null) {
          opcode = GeneratorAdapter.GE;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushCond(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBooleqne(JavascriptParser.BooleqneContext ctx) {
        int opcode;

        if (ctx.EQ() != null) {
          opcode = GeneratorAdapter.EQ;
        } else if (ctx.NE() != null) {
          opcode = GeneratorAdapter.NE;
        } else {
          throw new IllegalStateException("Unknown operation specified: " + ctx.getText());
        }

        pushCond(opcode, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwand(JavascriptParser.BwandContext ctx) {
        pushBitwise(Opcodes.LAND, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwxor(JavascriptParser.BwxorContext ctx) {
        pushBitwise(Opcodes.LXOR, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBwor(JavascriptParser.BworContext ctx) {
        pushBitwise(Opcodes.LOR, ctx.expression(0), ctx.expression(1));

        return null;
      }

      @Override
      public Void visitBooland(JavascriptParser.BoolandContext ctx) {
        Label andFalse = new Label();
        Label andEnd = new Label();

        typeStack.push(Type.INT_TYPE);
        visit(ctx.expression(0));
        gen.visitJumpInsn(Opcodes.IFEQ, andFalse);
        visit(ctx.expression(1));
        gen.visitJumpInsn(Opcodes.IFEQ, andFalse);
        typeStack.pop();
        pushBoolean(true);
        gen.goTo(andEnd);
        gen.visitLabel(andFalse);
        pushBoolean(false);
        gen.visitLabel(andEnd);

        return null;
      }

      @Override
      public Void visitBoolor(JavascriptParser.BoolorContext ctx) {
        Label orTrue = new Label();
        Label orEnd = new Label();

        typeStack.push(Type.INT_TYPE);
        visit(ctx.expression(0));
        gen.visitJumpInsn(Opcodes.IFNE, orTrue);
        visit(ctx.expression(1));
        gen.visitJumpInsn(Opcodes.IFNE, orTrue);
        typeStack.pop();
        pushBoolean(false);
        gen.goTo(orEnd);
        gen.visitLabel(orTrue);
        pushBoolean(true);
        gen.visitLabel(orEnd);

        return null;
      }

      @Override
      public Void visitConditional(JavascriptParser.ConditionalContext ctx) {
        Label condFalse = new Label();
        Label condEnd = new Label();

        typeStack.push(Type.INT_TYPE);
        visit(ctx.expression(0));
        typeStack.pop();
        gen.visitJumpInsn(Opcodes.IFEQ, condFalse);
        visit(ctx.expression(1));
        gen.goTo(condEnd);
        gen.visitLabel(condFalse);
        visit(ctx.expression(2));
        gen.visitLabel(condEnd);

        return null;
      }

      private void pushArith(int operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE);
      }

      private void pushShift(int operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, Type.LONG_TYPE, Type.INT_TYPE, Type.LONG_TYPE);
      }

      private void pushBitwise(int operator, ExpressionContext left, ExpressionContext right) {
        pushBinaryOp(operator, left, right, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
      }

      private void pushBinaryOp(
          int operator,
          ExpressionContext left,
          ExpressionContext right,
          Type leftType,
          Type rightType,
          Type returnType) {
        typeStack.push(leftType);
        visit(left);
        typeStack.pop();
        typeStack.push(rightType);
        visit(right);
        typeStack.pop();
        gen.visitInsn(operator);
        gen.cast(returnType, typeStack.peek());
      }

      private void pushCond(int operator, ExpressionContext left, ExpressionContext right) {
        Label labelTrue = new Label();
        Label labelReturn = new Label();

        typeStack.push(Type.DOUBLE_TYPE);
        visit(left);
        visit(right);
        typeStack.pop();

        gen.ifCmp(Type.DOUBLE_TYPE, operator, labelTrue);
        pushBoolean(false);
        gen.goTo(labelReturn);
        gen.visitLabel(labelTrue);
        pushBoolean(true);
        gen.visitLabel(labelReturn);
      }

      private void pushBoolean(boolean truth) {
        switch (typeStack.peek().getSort()) {
          case Type.INT:
            gen.push(truth);
            break;
          case Type.LONG:
            gen.push(truth ? 1L : 0L);
            break;
          case Type.DOUBLE:
            gen.push(truth ? 1. : 0.);
            break;
          default:
            throw new IllegalStateException("Invalid expected type: " + typeStack.peek());
        }
      }

      private void pushLong(long i) {
        switch (typeStack.peek().getSort()) {
          case Type.INT:
            gen.push((int) i);
            break;
          case Type.LONG:
            gen.push(i);
            break;
          case Type.DOUBLE:
            gen.push((double) i);
            break;
          default:
            throw new IllegalStateException("Invalid expected type: " + typeStack.peek());
        }
      }
    }.visit(parseTree);

    gen.returnValue();
    gen.endMethod();

    classWriter.visitEnd();
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
   * The default set of functions available to expressions.
   *
   * <p>See the {@link org.apache.lucene.expressions.js package documentation} for a list.
   */
  public static final Map<String, MethodHandle> DEFAULT_FUNCTIONS = loadDefaultFunctions();

  private static final Map<String, MethodHandle> loadDefaultFunctions() {
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
    // try to crack the handle and check if it is a static call:
    int refKind;
    try {
      refKind = EXPRESSION_LOOKUP.revealDirect(method).getReferenceKind();
    } catch (
        @SuppressWarnings("unused")
        IllegalArgumentException iae) {
      // can't check for static, we assume it is static
      // (it does not matter as we call the MethodHandle directly if it is compatible):
      refKind = MethodHandleInfo.REF_invokeStatic;
    }
    if (refKind != MethodHandleInfo.REF_invokeStatic && refKind != MethodHandleInfo.REF_getStatic) {
      throw new IllegalArgumentException(method + " is not static.");
    }

    // do some checks if the signature is "compatible":
    final MethodType type = method.type();
    for (int arg = 0, arity = type.parameterCount(); arg < arity; arg++) {
      if (type.parameterType(arg) != double.class) {
        throw new IllegalArgumentException(method + " must take only double parameters.");
      }
    }
    if (type.returnType() != double.class) {
      throw new IllegalArgumentException(method + " does not return a double.");
    }
  }

  /**
   * Generates a valid Java constant name from the function name. All invalid Java Identifier
   * characters are replaced by {@code "$"} and the hashCode is added for uniqueness.
   */
  private static String getMethodHandleConstantName(String functionName) {
    return "MH_"
        + functionName.replaceAll("\\P{javaJavaIdentifierPart}", "\\$")
        + "_"
        + Integer.toUnsignedString(functionName.hashCode());
  }

  /**
   * Bootstrap method for dynamic constants. This returns a {@link MethodHandle} for the {@code
   * functionName} from the class data passed via {@link Lookup#defineHiddenClassWithClassData}. The
   * {@code constantName} is ignored.
   */
  static MethodHandle dynamicConstantBootstrap(
      Lookup lookup, String constantName, Class<?> type, String functionName)
      throws IllegalAccessException {
    if (type != MethodHandle.class) {
      throw new IllegalArgumentException("Invalid type of constant: " + type.getName());
    }
    final var classData =
        Objects.requireNonNull(
            MethodHandles.classData(lookup, ConstantDescs.DEFAULT_NAME, Map.class),
            "Missing class data for " + lookup);
    return (MethodHandle)
        Objects.requireNonNull(
            classData.get(functionName), "Function does not exist: " + functionName);
  }
}
