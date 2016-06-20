package com.ociweb.hazelcast.util;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

import static org.junit.Assert.*;

public class ValidateTemplates {

    // TODO: reset the templateFile when Set is working.
    // private static final String templateFile = "/HazelcastRequests.xml";
    private static final String templateFile = "/HazelcastSetRequests.xml";


    @Test
   public void testCodeGeneration() throws IOException, ParserConfigurationException, SAXException {

        //NOTE: this is specific to HZ, TODO, do not keep this data in this test.
       String[] feature = new String[]{"Client",   //0x00
                                        "Map",//0x01
                                        "Multimap",//0x02
                                        "Queue",//0x03
                                        "Topic",//0x04
                                        "List",//0x05
                                        "Set",//0x06
                                        "Lock",//0x07
                                        "Condition",//0x08
                                        "ExecutorService",//0x09
                                        "AtomicLong",//0x0a
                                        "AtomicReference",//0x0b
                                        "CountDownLatch",//0x0c
                                        "Semaphore",//0x0d
                                        "ReplicatedMap",//0x0e
                                        "JobProcess",//0x0f
                                        "TransactionMap",//0x10
                                        "TransactionMultimap",//0x11
                                        "TransactionalSet",//0x12
                                        "TransactionList",//0x13
                                        "TransactionalQueue",//0x14
                                        "Cache",//0x15
                                        null,//0x16
                                        "Transaction", //0x17
                                        null};


        final String[] paraTypes = new String[] {
               "int", "int", "int", "int", "long", "long", "long", "long",
               "CharSequence", "CharSequence", "CharSequence", "CharSequence",
               "undef:decimal", "undef:decimal",
               "ByteBuffer", "ByteBuffer",
               "undef:group", "undef:Reserved1", "undef:Reserved2", "undef:Reserved3", "int", "undef:Reserved5",
               "undef:Reserved6", "undef:Reserved7", "undef:Dictionary" };


        String writerName = PipeWriter.class.getSimpleName();

       //TODO: Return correlation call? force correlation value in?

        //if this is ThreadId do not expose this in the method signature
        //TODO: may be a very bad idea because client needs same ID to release.  Change to lockID name.  lockPasscode


       String tab = "    ";

       //Build up all the string builders so we can group the methods regardless of the order in which they are defined.
       int featureCount = feature.length;
       final StringBuilder[] methods = new StringBuilder[featureCount];
       int f = featureCount;
       while (--f>=0) {
           methods[f] = new StringBuilder();
       }

       FieldReferenceOffsetManager from = TemplateHandler.loadFrom(templateFile);

     //  encodedFrom.dictionaryNameScript

       int limit = from.messageStarts.length;
       int i = 0;
       while (i<limit) {
           int msgIdx = from.messageStarts[i];

           f = (int)from.fieldIdScript[msgIdx] >> 8; //NOTE: this is specific to HZ
           assert(f<32);


           Appendable target = methods[f];

           target.append("\n\n").append(tab);

           appendValidJavaMethodName(target.append("public static boolean "), from.fieldNameScript[msgIdx] );

           target.append('(');
           {//build all the parameters
               int fieldsSize = from.fragScriptSize[msgIdx]-1; //for template id
               int k = 1;
               boolean notFirst = false;
               while (k<fieldsSize) {

                   int idx = msgIdx+k;
                   int token = from.tokens[idx];
                   int type = TokenBuilder.extractType(token);

                   System.err.println(from.fieldNameScript[idx]+" next "+from.fieldNameScript[idx+1]);

                   if (0x1fffef == from.fieldIdScript[idx]) { //PartitionHash constant TODO: extract as static final
                       //this value is computed in the method not passed in
                   } else {
                       if (notFirst) {
                           target.append(", ");
                       } else {
                           notFirst = true;
                       }
                       appendValidJavaMethodName(target, from.fieldNameScript[idx]).append(' ').append(paraTypes[type]);
                   }

                   k += TypeMask.scriptTokenSize[type];

               }
           }
           //TODO: duplicate the writeUTF8 for writing constants with zero copy, then use it here to build multiple method version.
           //      runtime ring, uses this generated template and adds constants values to be used.

           target.append(") {\n").append(tab);
           int fieldsSize = from.fragScriptSize[msgIdx]-1; //for template id

           if (fieldsSize<=1) {
               //simple case where there are no fields
               target.append(tab).append("return ").append(writerName).append(".tryWriteFragment(pipe, 0x").append(Integer.toHexString(msgIdx)).append(");\n").append(tab);

           } else {

               target.append(tab).append("if (").append(writerName).append(".tryWriteFragment(pipe, 0x").append(Integer.toHexString(msgIdx)).append(") {\n").append(tab);

               //build the method body.
               writeMethodBody(writerName, tab, from, msgIdx, target, fieldsSize);

               target.append(tab).append(tab).append("return true;\n").append(tab);
               target.append(tab).append("} else {\n").append(tab);
               target.append(tab).append(tab).append("return false;\n").append(tab);
               target.append(tab).append("}\n").append(tab);
           }
           target.append("}\n");


           i++;
       }

       StringBuilder result = new StringBuilder();
       f = featureCount;
       while (--f>=0) {
           if (null!=feature[f] && null!=methods[f] && methods[f].length()>0) {
               result.append("\n\n");

               appendValidJavaClassName(result.append("public static class "), feature[f] ).append(" {\n");

               result.append(methods[f]);

               result.append("}\n");

           }
       }




      //Show the result
     System.out.println(result);

   }

    StringBuilder workspace = new StringBuilder();

    private void writeMethodBody(String writerName, String tab, FieldReferenceOffsetManager from, int msgIdx,
                                 Appendable target, int fieldsSize) throws IOException {

           int k = 1;
           while (k<fieldsSize) {
               int idx = msgIdx+k;
               int token = from.tokens[idx];
               int type = TokenBuilder.extractType(token);
               int loc = FieldReferenceOffsetManager.lookupFieldLocator(from.fieldIdScript[idx], msgIdx, from);

               boolean inlineHashGenerator = false;
               String pHashGenerator = "-1"; //default hash for those things without keys
               if (0x1fffef == from.fieldIdScript[idx]) { //TODO: extract constant for partition hash
                   inlineHashGenerator = true;


                   //do we have a key to use for building the hash
                   int j = 1;
                   while (j<fieldsSize) {

                       int jidx = msgIdx+j;
                       int jtype = TokenBuilder.extractType(from.tokens[jidx]);

                       if ("Key".equals(from.fieldNameScript[jidx])) {

                           workspace.setLength(0);
                           appendValidJavaMethodName(workspace.append("computeHash("), from.fieldNameScript[jidx]).append(");");
                           pHashGenerator = workspace.toString();

                       }
                       j += TypeMask.scriptTokenSize[jtype];
                   }

               }


               switch (type) {
                   case TypeMask.IntegerSigned:
                   case TypeMask.IntegerUnsigned:
                   case TypeMask.IntegerSignedOptional:
                   case TypeMask.IntegerUnsignedOptional:
                       target.append(tab).append(tab).append(writerName).append(".writeInt(pipe, 0x").append(Integer.toHexString(loc)).append(", ");
                       if (inlineHashGenerator) {
                           target.append(pHashGenerator);
                       } else {
                           appendValidJavaMethodName(target, from.fieldNameScript[idx]);
                       }
                       target.append(");\n").append(tab);
                       break;
                   case TypeMask.LongSigned:
                   case TypeMask.LongSignedOptional:
                   case TypeMask.LongUnsigned:
                   case TypeMask.LongUnsignedOptional:
                       target.append(tab).append(tab).append(writerName).append(".writeLong(pipe, ");
                       target.append("0x").append(Long.toHexString(loc));
                       appendValidJavaMethodName(target.append(", "), from.fieldNameScript[idx]).append(");\n").append(tab);
                       break;
                   case TypeMask.TextASCII:
                   case TypeMask.TextASCIIOptional:
                   case TypeMask.TextUTF8:
                   case TypeMask.TextUTF8Optional:
                       //we only support UTF8 for HZ so all these will be encoded at UTF8
                       target.append(tab).append(tab).append(writerName).append(".writeUTF8(pipe, 0x").append(Long.toHexString(loc)).append(", ");
                       appendValidJavaMethodName(target, from.fieldNameScript[idx]).append(");\n").append(tab);
                       break;
                   case TypeMask.ByteVector:
                   case TypeMask.ByteVectorOptional:
                       target.append(tab).append(tab).append(writerName).append(".writeBytes(pipe, 0x").append(Long.toHexString(loc)).append(", ");
                       appendValidJavaMethodName(target, from.fieldNameScript[idx]).append(");\n").append(tab);
                       break;
                   default:
                       throw new UnsupportedOperationException("Unimplemented feature for suppport of "+TokenBuilder.tokenToString(token));
               }

               k += TypeMask.scriptTokenSize[type];

           }
    }

    /**
     * Builds a valid method name out of the provided string
     * @param target
     * @param source
     * @return
     * @throws IOException
     */
    private Appendable appendValidJavaMethodName(Appendable target, String source) throws IOException {
        if (null==source) {
            target.append("NULL");
        } else {
            target.append(Character.toLowerCase(source.charAt(0)));
            int i = 1;
            int len = source.length();
            while (i<len) {
                char c = source.charAt(i++);
                if (!Character.isWhitespace(c)) {
                    target.append(c);
                }
            }
        }
        return target;
    }

    private Appendable appendValidJavaClassName(Appendable target, String source) throws IOException {
        if (null==source) {
            target.append("NULL");
        } else {
            int i = 0;
            int len = source.length();
            while (i<len) {
                char c = source.charAt(i++);
                if (!Character.isWhitespace(c)) {
                    target.append(c);
                }
            }
        }
        return target;
    }

}
