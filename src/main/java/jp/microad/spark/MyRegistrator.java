package jp.microad.spark;

import org.apache.spark.serializer.KryoRegistrator;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.analysis.Tokenizer;

import scala.collection.mutable.WrappedArray.ofRef;

import com.esotericsoftware.kryo.Kryo;
import com.mad.ContentExtractor.ContentExtractor;

public class MyRegistrator implements KryoRegistrator {
    public void registerClasses(Kryo kryo) {
    	  kryo.register(Object[].class);
    	  kryo.register(ofRef.class);
    	  kryo.register(java.util.ArrayList.class);
    	  kryo.register(RowInfo.class);
    	  
//          kryo.register(ContentExtractor.class);
//          kryo.register(JapaneseTokenizer.class);
//          kryo.register(Arc.class);
//          kryo.register(java.util.LinkedHashMap.class);
//          kryo.register(java.lang.Class.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.PackedTokenAttributeImpl.class);
//          kryo.register(org.apache.lucene.util.BytesRefBuilder.class);
//          kryo.register(org.apache.lucene.util.BytesRef.class);
//          kryo.register(char[].class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.BaseFormAttributeImpl.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.PartOfSpeechAttributeImpl.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.ReadingAttributeImpl.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.InflectionAttributeImpl.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.TypeAttribute.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.OffsetAttribute.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);
//          kryo.register(org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.BaseFormAttribute.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.ReadingAttribute.class);
//          kryo.register(org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.InflectionAttribute.class);
//          kryo.register(org.apache.lucene.analysis.util.RollingCharBuffer.class);

    }
}
