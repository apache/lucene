package org.apache.lucene.util.bkd.docIds;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public interface DocIdEncoder {

    public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException;

    public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException;

    static class Factory {

        public static DocIdEncoder fromName(String encoderName) {
            String parsedEncoderName = encoderName.trim();
            if (parsedEncoderName.equalsIgnoreCase(Bit24Encoder.class.getSimpleName())) {
                return new Bit24Encoder();
            } else if (parsedEncoderName.equalsIgnoreCase(Bit21With3StepsAddEncoder.class.getSimpleName())) {
                return new Bit21With3StepsAddEncoder();
            } else if (parsedEncoderName.equalsIgnoreCase(Bit21WithArrEncoder.class.getSimpleName())) {
                return new Bit21WithArrEncoder();
            } else if (parsedEncoderName.equalsIgnoreCase(Bit21With2StepsAddAndShortByteEncoder.class.getSimpleName())) {
                return new Bit21With2StepsAddAndShortByteEncoder();
            } else if (parsedEncoderName.equalsIgnoreCase(Bit21With3StepsAddAndShortByteEncoder.class.getSimpleName())) {
                return new Bit21With3StepsAddAndShortByteEncoder();
            } else if (parsedEncoderName.equalsIgnoreCase(Bit21With2StepsAddEncoder.class.getSimpleName())) {
                return new Bit21With2StepsAddEncoder();
            } else {
                throw new IllegalArgumentException("Unknown DocIdEncoder " + encoderName);
            }
        }

    }

}
