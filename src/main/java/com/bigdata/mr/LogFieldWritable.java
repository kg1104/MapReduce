package com.bigdata.mr;

import org.apache.hadoop.io.*;

public class LogFieldWritable extends GenericWritable {
    public LogFieldWritable() { set(NullWritable.get()); }

    public LogFieldWritable(Object obj) {
        if (obj == null) {
            set(NullWritable.get());
        } else if (obj instanceof Long) {
            set(new LongWritable((Long)obj));
        } else if (obj instanceof Double) {
            set(new DoubleWritable((Double) obj));
        }  else if (obj instanceof Text) {
            set(new Text((String) obj));
        } else {
            throw new RuntimeException("Format not support");
        }
    }

    protected Class<? extends Writable>[] getTypes() {
        return new Class[] {Text.class, LongWritable.class, NullWritable.class, DoubleWritable.class};
    }

    public Object getObject() {
        Writable w = get();
        if (w instanceof Text) {
            return w.toString();
        } else if (w instanceof LongWritable) {
            return ((LongWritable)w).get();
        } else if (w instanceof DoubleWritable) {
            return ((DoubleWritable)w).get();
        } else {
            return null;
        }
    }
}
