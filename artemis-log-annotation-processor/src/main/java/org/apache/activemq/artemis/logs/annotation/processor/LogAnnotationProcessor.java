/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.logs.annotation.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.activemq.artemis.logs.annotation.GetLogger;
import org.apache.activemq.artemis.logs.annotation.LogBundle;
import org.apache.activemq.artemis.logs.annotation.LogMessage;
import org.apache.activemq.artemis.logs.annotation.Message;

@SupportedAnnotationTypes({"org.apache.activemq.artemis.logs.annotation.LogBundle"})
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class LogAnnotationProcessor extends AbstractProcessor {
   private static final boolean DEBUG;

   static {
      boolean debugResult = false;
      try {
         String debugEnvVariable = System.getenv("ARTEMIS_LOG_ANNOTATION_PROCESSOR_DEBUG");
         if (debugEnvVariable != null) {
            debugResult = Boolean.parseBoolean(debugEnvVariable);
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
      DEBUG = debugResult;
   }

   /*
    * define environment variable ARTEMIS_LOG_ANNOTATION_PROCESSOR_DEBUG=true in order to see debug output
    */
   protected static void debug(String debugMessage) {
      if (DEBUG) {
         System.out.println(debugMessage);
      }
   }

   @Override
   public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      HashMap<Integer, String> messages = new HashMap<>();

      try {
         for (TypeElement annotation : annotations) {
            for (Element annotatedTypeEl : roundEnv.getElementsAnnotatedWith(annotation)) {
               TypeElement annotatedType = (TypeElement) annotatedTypeEl;

               LogBundle bundleAnnotation = annotatedType.getAnnotation(LogBundle.class);

               // Validate the retiredIDs, if any, are valid and pre-sorted
               validateRetiredIDsAreValidAndSorted(annotatedType, bundleAnnotation);

               // Collect all the active IDs for this LogBundle
               final List<Integer> activeIDs = collectActiveIDs(annotatedType);

               String fullClassName = annotatedType.getQualifiedName() + "_impl";
               String interfaceName = annotatedType.getSimpleName().toString();
               String simpleClassName = interfaceName + "_impl";
               JavaFileObject fileObject = processingEnv.getFiler().createSourceFile(fullClassName);

               if (DEBUG) {
                  debug("");
                  debug("*******************************************************************************************************************************");
                  debug("processing " + fullClassName + ", generating: " + fileObject.getName());
               }

               PrintWriter writerOutput = new PrintWriter(fileObject.openWriter());

               // header
               writerOutput.println("/* This class is auto generated by " + LogAnnotationProcessor.class.getCanonicalName());
               writerOutput.println("   and it inherits whatever license is declared at " + annotatedType + " */");
               writerOutput.println();

               // opening package
               writerOutput.println("package " + annotatedType.getEnclosingElement() + ";");
               writerOutput.println();

               writerOutput.println("import org.slf4j.Logger;");
               writerOutput.println("import org.slf4j.LoggerFactory;");
               writerOutput.println("import org.slf4j.helpers.FormattingTuple;");
               writerOutput.println("import org.slf4j.helpers.MessageFormatter;");

               writerOutput.println();

               // Opening class
               writerOutput.println("// " + bundleAnnotation.toString());
               writerOutput.println("public class " + simpleClassName + " implements " + interfaceName);
               writerOutput.println("{");

               writerOutput.println("   private final Logger logger;");
               writerOutput.println();

               writerOutput.println("   private static void _copyStackTraceMinusOne(final Throwable e) {");
               writerOutput.println("      final StackTraceElement[] st = e.getStackTrace();");
               writerOutput.println("      e.setStackTrace(java.util.Arrays.copyOfRange(st, 1, st.length));");
               writerOutput.println("   }");
               writerOutput.println();

               writerOutput.println("   public " + simpleClassName + "(Logger logger ) {");
               writerOutput.println("      this.logger = logger;");
               writerOutput.println("   }");
               writerOutput.println();

               for (Element el : annotatedType.getEnclosedElements()) {
                  if (el.getKind() == ElementKind.METHOD) {
                     ExecutableElement executableMember = (ExecutableElement) el;

                     // If adding any new types, update collectActiveIDs method
                     Message messageAnnotation = el.getAnnotation(Message.class);
                     LogMessage logAnnotation = el.getAnnotation(LogMessage.class);
                     GetLogger getLogger = el.getAnnotation(GetLogger.class);

                     if (DEBUG) {
                        debug("Generating " + executableMember);
                     }

                     int generatedPaths = 0;

                     if (messageAnnotation != null) {
                        validateRegexID(bundleAnnotation, messageAnnotation.id());
                        generatedPaths++;
                        if (DEBUG) {
                           debug("... annotated with " + messageAnnotation);
                        }
                        generateMessage(bundleAnnotation, writerOutput, executableMember, messageAnnotation, messages, activeIDs);
                     }

                     if (logAnnotation != null) {
                        validateRegexID(bundleAnnotation, logAnnotation.id());
                        generatedPaths++;
                        if (DEBUG) {
                           debug("... annotated with " + logAnnotation);
                        }
                        generateLogger(bundleAnnotation, writerOutput, executableMember, logAnnotation, messages, activeIDs);
                     }

                     if (getLogger != null) {
                        generatedPaths++;
                        if (DEBUG) {
                           debug("... annotated with " + getLogger);
                        }
                        generateGetLogger(bundleAnnotation, writerOutput, executableMember, getLogger);
                     }

                     if (generatedPaths > 1) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Cannot use combined annotations  on " + executableMember);
                        return false;
                     }
                  }
               }

               writerOutput.println("}");
               writerOutput.close();

               if (DEBUG) {
                  debug("done processing " + fullClassName);
                  debug("*******************************************************************************************************************************");
                  debug("");
               }
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
         processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
         return false;
      }

      return true;
   }

   private static void validateRegexID(LogBundle bundleAnnotation, long id) {
      if (!isAllowedIDValue(bundleAnnotation, id)) {
         throw new IllegalArgumentException("Code " + id + " does not match regular expression \"" + bundleAnnotation.regexID() + "\" specified on the LogBundle");
      }
   }

   private static boolean isAllowedIDValue(LogBundle bundleAnnotation, long id) {
      if (bundleAnnotation.regexID() != null && !bundleAnnotation.regexID().isEmpty()) {
         String toStringID = Long.toString(id);

         return toStringID.matches(bundleAnnotation.regexID());
      }

      return true;
   }

   private static void generateMessage(LogBundle bundleAnnotation,
                                PrintWriter writerOutput,
                                ExecutableElement executableMember,
                                Message messageAnnotation,
                                HashMap<Integer, String> processedMessages,
                                List<Integer> activeIDs) {

      verifyIdNotRetiredOrProcessedPreviously(bundleAnnotation, executableMember, messageAnnotation.id(), messageAnnotation.value(), processedMessages, activeIDs);
      verifyMessagePlaceholders(messageAnnotation.value(), executableMember);

      processedMessages.put(messageAnnotation.id(), messageAnnotation.value());

      // This is really a debug output
      writerOutput.println("   // " + encodeSpecialChars(messageAnnotation.toString()));

      writerOutput.println("   @Override");
      writerOutput.write("   public " + executableMember.getReturnType() + " " + executableMember.getSimpleName() + "(");

      Iterator<? extends VariableElement> parameters = executableMember.getParameters().iterator();

      boolean hasParameters = false;
      VariableElement exceptionParameter = null;

      // the one that will be used on the call
      StringBuffer callList = new StringBuffer();
      while (parameters.hasNext()) {
         hasParameters = true;
         VariableElement parameter = parameters.next();

         boolean isException = verifyIfExceptionArgument(executableMember, parameter, parameters.hasNext(), exceptionParameter != null);
         if (isException) {
            exceptionParameter = parameter;
         }

         writerOutput.write(parameter.asType() + " " + parameter.getSimpleName());
         callList.append(parameter.getSimpleName());
         if (parameters.hasNext()) {
            writerOutput.write(", ");
            callList.append(", ");
         }
      }

      // the real implementation
      writerOutput.println(") {");

      String formattingString = encodeSpecialChars(bundleAnnotation.projectCode() + messageAnnotation.id() + ": " + messageAnnotation.value());
      if (!hasParameters) {
         writerOutput.println("      String returnString = \"" + formattingString + "\";");
      } else {
         writerOutput.println("      String returnString = MessageFormatter.arrayFormat(\"" + formattingString + "\", new Object[]{" + callList + "}).getMessage();");
      }

      if (executableMember.getReturnType().toString().equals(String.class.getName())) {
         writerOutput.println("      return returnString;");
      } else {
         writerOutput.println();
         writerOutput.println("      {");
         String exceptionVariableName = "objReturn_" + executableMember.getSimpleName();
         writerOutput.println("         " + executableMember.getReturnType().toString() + " " + exceptionVariableName + " = new " + executableMember.getReturnType().toString() + "(returnString);");

         if (exceptionParameter != null) {
            writerOutput.println("         " + exceptionVariableName + ".initCause(" + exceptionParameter.getSimpleName() + ");");
         }
         writerOutput.println("         _copyStackTraceMinusOne(" + exceptionVariableName + ");");
         writerOutput.println("         return " + exceptionVariableName + ";");
         writerOutput.println("      }");
      }

      writerOutput.println("   }");
      writerOutput.println();
   }

   private static boolean isException(TypeMirror parameterType, VariableElement methodParameter) {
      if (parameterType == null) {
         // This should never happen, but my OCD can't help here, I'm adding this just in case
         return false;
      }

      if (DEBUG && methodParameter != null) {
         debug("... checking if parameter \"" + parameterType + " " + methodParameter + "\" is an exception");
      }

      String parameterClazz = parameterType.toString();
      if (parameterClazz.equals("java.lang.Throwable") || parameterClazz.endsWith("Exception")) { // bad luck if you named a class with Exception and it was not an exception ;)
         if (DEBUG) {
            debug("... Class " + parameterClazz + " was considered an exception");
         }
         return true;
      }

      switch(parameterClazz) {
         // some known types
         case "java.lang.String":
         case "java.lang.Object":
         case "java.lang.Long":
         case "java.lang.Integer":
         case "java.lang.Number":
         case "java.lang.Thread":
         case "java.lang.ThreadGroup":
         case "org.apache.activemq.artemis.api.core.SimpleString":
         case "none":
            if (DEBUG) {
               debug("... " + parameterClazz + " is a known type, not an exception!");
            }
            return false;
      }

      if (parameterType instanceof DeclaredType) {
         DeclaredType declaredType = (DeclaredType) parameterType;
         if (declaredType.asElement() instanceof TypeElement) {
            TypeElement theElement = (TypeElement) declaredType.asElement();
            if (DEBUG) {
               debug("... ... recursively inspecting super class for Exception on " + parameterClazz + ", looking at superClass " + theElement.getSuperclass());
            }
            return isException(theElement.getSuperclass(), null);
         }
      }
      return false;
   }

   private static String encodeSpecialChars(String input) {
      return input.replaceAll("\n", "\\\\n").replaceAll("\"", "\\\\\"");
   }


   private static void generateGetLogger(LogBundle bundleAnnotation,
                                  PrintWriter writerOutput,
                                  ExecutableElement executableMember,
                                  GetLogger loggerAnnotation) {

      // This is really a debug output
      writerOutput.println("   // " + loggerAnnotation.toString());
      writerOutput.println("   @Override");
      writerOutput.println("   public Logger " + executableMember.getSimpleName() + "() {");
      writerOutput.println("      return logger;");
      writerOutput.println("   }");
      writerOutput.println();
   }


   private static void generateLogger(LogBundle bundleAnnotation,
                               PrintWriter writerOutput,
                               ExecutableElement executableMember,
                               LogMessage messageAnnotation,
                               HashMap<Integer, String> processedMessages,
                               List<Integer> activeIDs) {

      verifyIdNotRetiredOrProcessedPreviously(bundleAnnotation, executableMember, messageAnnotation.id(), messageAnnotation.value(), processedMessages, activeIDs);
      verifyMessagePlaceholders(messageAnnotation.value(), executableMember);

      processedMessages.put(messageAnnotation.id(), messageAnnotation.value());

      // This is really a debug output
      writerOutput.println("   // " + encodeSpecialChars(messageAnnotation.toString()));
      writerOutput.println("   @Override");
      writerOutput.write("   public void " + executableMember.getSimpleName() + "(");

      List<? extends VariableElement> parametersList = executableMember.getParameters();

      boolean hasParameters = false;
      VariableElement exceptionParameter = null;

      Iterator<? extends VariableElement> parameters = parametersList.iterator();
      while (parameters.hasNext()) {
         hasParameters = true;
         VariableElement parameter = parameters.next();

         boolean isException = verifyIfExceptionArgument(executableMember, parameter, parameters.hasNext(), exceptionParameter != null);
         if (isException) {
            exceptionParameter = parameter;
         }

         writerOutput.write(parameter.asType() + " " + parameter.getSimpleName());
         if (parameters.hasNext()) {
            writerOutput.write(", ");
         }
      }

      writerOutput.println(") {");

      StringBuffer callList = null;
      if (hasParameters) {
         // the one that will be used on the logger call
         callList = new StringBuffer();

         parameters = parametersList.iterator();
         while (parameters.hasNext()) {
            VariableElement parameter = parameters.next();
            callList.append(parameter.getSimpleName());
            if (parameters.hasNext()) {
               callList.append(", ");
            }
         }
      }

      final String isEnabledMethodName = getLoggerIsEnabledMethodName(messageAnnotation);
      final String methodName = getLoggerOutputMethodName(messageAnnotation);
      final String formattingString = encodeSpecialChars(bundleAnnotation.projectCode() + messageAnnotation.id() + ": " + messageAnnotation.value());

      writerOutput.println("      if (logger." + isEnabledMethodName + "()) {");

      if (hasParameters) {
         writerOutput.println("         logger." + methodName + "(\"" + formattingString + "\", " + callList + ");");
      } else {
         writerOutput.println("         logger." + methodName + "(\"" + formattingString + "\");");
      }

      writerOutput.println("      }");

      writerOutput.println("   }");
      writerOutput.println();
   }

   private static String getLoggerOutputMethodName(LogMessage messageAnnotation) {
      switch (messageAnnotation.level()) {
         case WARN:
            return "warn";
         case INFO:
            return "info";
         case ERROR:
            return "error";
         case DEBUG:
            return "debug";
         case TRACE:
            return  "trace";
         default:
            throw new IllegalStateException("Illegal log level: " + messageAnnotation.level());
      }
   }

   private static String getLoggerIsEnabledMethodName(LogMessage messageAnnotation) {
      switch (messageAnnotation.level()) {
         case WARN:
            return "isWarnEnabled";
         case INFO:
            return "isInfoEnabled";
         case ERROR:
            return "isErrorEnabled";
         case DEBUG:
            return "isDebugEnabled";
         case TRACE:
            return "isTraceEnabled";
         default:
            throw new IllegalStateException("Illegal log level: " + messageAnnotation.level());
      }
   }

   private static void tupples(String arg, char open, char close, Consumer<String> stringConsumer) {
      int openAt = -1;
      for (int i = 0; i < arg.length(); i++) {
         char charAt = arg.charAt(i);

         if (charAt == open) {
            openAt = i;
         } else if (charAt == close) {
            if (openAt >= 0) {
               stringConsumer.accept(arg.substring(openAt + 1, i));
            }
            openAt = -1;
         }
      }
   }

   private static void verifyMessagePlaceholders(final String message, final ExecutableElement holder) {
      Objects.requireNonNull(message, "message must not be null");

      tupples(message, '{', '}', (tupple) -> {
         if (!tupple.equals("")) {
            throw new IllegalArgumentException("Invalid placeholder argument {" + tupple + "} on message \'" + message + "\' as part of " + holder + "\nreplace it by {}");
         }
      });

      if (message.contains("%s") || message.contains("%d")) {
         throw new IllegalArgumentException("Cannot use %s or %d in loggers. Please use {} on message \'" + message + "\'");
      }
   }

   private static void verifyIdNotRetiredOrProcessedPreviously(final LogBundle bundleAnnotation, final ExecutableElement executableMember, final Integer id, final String message, final HashMap<Integer, String> processedMessages, final List<Integer> activeIDs) {
      Objects.requireNonNull(id, "id must not be null");

      boolean retiredID = isRetiredID(bundleAnnotation, id);
      if (processedMessages.containsKey(id) || retiredID) {
         StringBuilder failure = new StringBuilder();

         failure.append(executableMember.getEnclosingElement().toString()).append(": ");

         if (processedMessages.containsKey(id)) {
            String previousMessage = processedMessages.get(id);

            failure.append("ID ").append(id)
                   .append(" with message '").append(message)
                   .append("' was previously used already, to define message '").append(previousMessage)
                   .append("'. ");
         }

         if (retiredID) {
            failure.append("ID ").append(id).append(" was previously retired, another ID must be used. ");
         }

         Integer nextId = Collections.max(activeIDs) + 1;
         while (isRetiredID(bundleAnnotation, nextId) ) {
            nextId++;
         }

         if (isAllowedIDValue(bundleAnnotation, nextId)) {
            failure.append("Consider trying ID ").append(nextId).append(" which is the next unused value.");
         } else {
            failure.append("There are no new IDs available within the given ID regex: " + bundleAnnotation.regexID());
         }

         throw new IllegalStateException(failure.toString());
      }
   }

   private static boolean isRetiredID(final LogBundle bundleAnnotation, final Integer id) {
      return Arrays.binarySearch(bundleAnnotation.retiredIDs(), id) >= 0;
   }

   private static boolean verifyIfExceptionArgument(final ExecutableElement executableMember, final VariableElement parameter, final boolean hasMoreParams, final boolean hasExistingException) {
      boolean isException = isException(parameter.asType(), parameter);
      if (DEBUG) {
         debug("Parameter " + parameter + (isException ? "is" : "is not") + " an exception");
      }

      if (isException) {
         if (hasMoreParams) {
            throw new IllegalArgumentException("Exception argument " + parameter + " has to be the last argument on the list. Look at: " + executableMember);
         }

         if (hasExistingException) {
            throw new IllegalStateException("You can only have one exception argument defined per message/annotation, Look at: " + executableMember);
         }
      }

      return isException;
   }

   private static void validateRetiredIDsAreValidAndSorted(TypeElement annotatedType, LogBundle bundleAnnotation) {
      int[] retiredIDs = bundleAnnotation.retiredIDs();
      if (retiredIDs.length == 0) {
         // Nothing to check
         return;
      }

      for (int id : retiredIDs) {
         if (!isAllowedIDValue(bundleAnnotation, id)) {
            throw new IllegalArgumentException(annotatedType + ": The retiredIDs elements must each match the configured regexID. The ID " + id + " does not match: " + bundleAnnotation.regexID());
         }
      }

      int[] sortedRetiredIDs = Arrays.copyOf(retiredIDs, retiredIDs.length);
      Arrays.sort(sortedRetiredIDs);

      if (!Arrays.equals(retiredIDs, sortedRetiredIDs)) {
         StringBuilder sortedIDsMessage = new StringBuilder();
         sortedIDsMessage.append("{");

         int count = 1;
         for (int id : sortedRetiredIDs) {
            sortedIDsMessage.append(id);

            if (count != sortedRetiredIDs.length) {
               sortedIDsMessage.append(", ");
               count++;
            }
         }

         sortedIDsMessage.append("}");

         throw new IllegalArgumentException(annotatedType + ": The retiredIDs value must be sorted. Try using: " + sortedIDsMessage.toString());
      }

      debug("Found retired IDs: " + Arrays.toString(retiredIDs));
   }

   private static List<Integer> collectActiveIDs(TypeElement annotatedType) {
      List<Integer> activeIds = new ArrayList<>();

      for (Element el : annotatedType.getEnclosedElements()) {
         if (el.getKind() == ElementKind.METHOD) {
            Message messageAnnotation = el.getAnnotation(Message.class);
            if (messageAnnotation != null) {
               activeIds.add(messageAnnotation.id());
            }

            LogMessage logAnnotation = el.getAnnotation(LogMessage.class);
            if (logAnnotation != null) {
               activeIds.add(logAnnotation.id());
            }
         }
      }

      activeIds.sort(null);

      debug("Found active IDs: " + activeIds);

      return activeIds;
   }
}
