package com.thinkaurelius.faunus;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.ReadByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement implements Element, WritableComparable<FaunusElement> {

    static {
        WritableComparator.define(FaunusElement.class, new Comparator());
    }

    protected static final KryoSerializer serialize = new KryoSerializer();

    protected static final Map<String, String> TYPE_MAP = new HashMap<String, String>() {
        @Override
        public final String get(final Object object) {
            final String label = (String) object;
            final String existing = super.get(label);
            if (null == existing) {
                super.put(label, label);
                return label;
            } else {
                return existing;
            }
        }
    };

    protected long id;
    protected Map<String, Object> properties = null;
    protected List<List<MicroElement>> paths = null;
    private MicroElement microVersion = null;
    protected boolean pathEnabled = false;
    protected long pathCounter = 0;


    public FaunusElement(final long id) {
        this.id = id;
    }

    protected FaunusElement reuse(final long id) {
        this.id = id;
        this.properties = null;
        this.clearPaths();
        return this;
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        //TODO: should this be supported?
        throw new UnsupportedOperationException();
    }

    public void enablePath(final boolean enablePath) {
        this.pathEnabled = enablePath;
        if (this.pathEnabled) {
            if (null == this.microVersion)
                this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
            if (null == this.paths)
                this.paths = new ArrayList<List<MicroElement>>();
        }
        // TODO: else make pathCounter = paths.size()?
    }

    public void addPath(final List<MicroElement> path, final boolean append) throws IllegalStateException {
        if (this.pathEnabled) {
            if (append) path.add(this.microVersion);
            this.paths.add(path);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public void addPaths(final List<List<MicroElement>> paths, final boolean append) throws IllegalStateException {
        if (this.pathEnabled) {
            if (append) {
                for (final List<MicroElement> path : paths) {
                    this.addPath(path, append);
                }
            } else
                this.paths.addAll(paths);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public List<List<MicroElement>> getPaths() throws IllegalStateException {
        if (this.pathEnabled)
            return this.paths;
        else
            throw new IllegalStateException("Path calculations are not enabled");
    }

    public void getPaths(final FaunusElement element, final boolean append) {
        if (this.pathEnabled) {
            this.addPaths(element.getPaths(), append);
        } else {
            this.pathCounter = this.pathCounter + element.pathCount();
        }
    }

    public long incrPath(final long amount) throws IllegalStateException {
        if (this.pathEnabled)
            throw new IllegalStateException("Path calculations are enabled -- use addPath()");
        else
            this.pathCounter = this.pathCounter + amount;
        return this.pathCounter;
    }

    public boolean hasPaths() {
        if (this.pathEnabled)
            return !this.paths.isEmpty();
        else
            return this.pathCounter > 0;
    }

    public void clearPaths() {
        if (this.pathEnabled) {
            this.paths = new ArrayList<List<MicroElement>>();
            this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
        } else
            this.pathCounter = 0;
    }

    public long pathCount() {
        if (this.pathEnabled)
            return this.paths.size();
        else
            return this.pathCounter;
    }

    public void startPath() {
        if (this.pathEnabled) {
            this.clearPaths();
            final List<MicroElement> startPath = new ArrayList<MicroElement>();
            startPath.add(this.microVersion);
            this.paths.add(startPath);
        } else {
            this.pathCounter = 1;
        }
    }

    public void setProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        if (key.equals(Tokens._COUNT))
            throw new IllegalArgumentException("_count is a reserved property");

        if (null == this.properties)
            this.properties = new HashMap<String, Object>();
        this.properties.put(TYPE_MAP.get(key), value);
    }

    public <T> T removeProperty(final String key) {
        return null == this.properties ? null : (T) this.properties.remove(key);
    }

    public <T> T getProperty(final String key) {
        if (key.equals(Tokens._COUNT))
            return (T) Long.valueOf(this.pathCount());
        return null == this.properties ? null : (T) this.properties.get(key);
    }

    public Set<String> getPropertyKeys() {
        return null == this.properties ? (Set) Collections.emptySet() : this.properties.keySet();
    }

    public Map<String, Object> getProperties() {
        return null == this.properties ? this.properties = new HashMap<String, Object>() : this.properties;
    }

    public Object getId() {
        return this.id;
    }

    public long getIdAsLong() {
        return this.id;
    }

    public void readFields(final DataInput in) throws IOException {
        this.id = WritableUtils.readVLong(in);
        this.pathEnabled = in.readBoolean();
        if (this.pathEnabled) {
            this.paths = ElementPaths.readFields(in);
            this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
        } else
            this.pathCounter = WritableUtils.readVLong(in);
        this.properties = ElementProperties.readFields(in);
    }

    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, this.id);
        out.writeBoolean(this.pathEnabled);
        if (this.pathEnabled)
            ElementPaths.write(this.paths, out);
        else
            WritableUtils.writeVLong(out, this.pathCounter);
        ElementProperties.write(this.properties, out);
    }

    @Override
    public boolean equals(final Object other) {
        return this.getClass().equals(other.getClass()) && this.id == ((FaunusElement) other).getIdAsLong();
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }

    public int compareTo(final FaunusElement other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }

    public static class ElementProperties {

        public static void write(final Map<String, Object> properties, final DataOutput out) throws IOException {
            if (null == properties || properties.size() == 0)
                WritableUtils.writeVInt(out, 0);
            else {
                WritableUtils.writeVInt(out, properties.size());
                final com.thinkaurelius.titan.graphdb.database.serialize.DataOutput o = serialize.getDataOutput(128, true);
                for (final Map.Entry<String, Object> entry : properties.entrySet()) {
                    o.writeObject(entry.getKey(), String.class);
                    o.writeClassAndObject(entry.getValue());
                }
                final StaticBuffer buffer = o.getStaticBuffer();
                WritableUtils.writeVInt(out, buffer.length());
                out.write(ByteBufferUtil.getArray(buffer.asByteBuffer()));
            }
        }

        public static Map<String, Object> readFields(final DataInput in) throws IOException {
            final int numberOfProperties = WritableUtils.readVInt(in);
            if (numberOfProperties == 0)
                return null;
            else {
                final Map<String, Object> properties = new HashMap<String, Object>();
                byte[] bytes = new byte[WritableUtils.readVInt(in)];
                in.readFully(bytes);
                final ReadBuffer buffer = new ReadByteBuffer(bytes);
                for (int i = 0; i < numberOfProperties; i++) {
                    final String key = serialize.readObject(buffer, String.class);
                    final Object valueObject = serialize.readClassAndObject(buffer);
                    properties.put(TYPE_MAP.get(key), valueObject);
                }
                return properties;
            }
        }
    }

    public static class ElementPaths {

        public static void write(final List<List<MicroElement>> paths, final DataOutput out) throws IOException {
            if (null == paths) {
                WritableUtils.writeVInt(out, 0);
            } else {
                WritableUtils.writeVInt(out, paths.size());
                for (final List<MicroElement> path : paths) {
                    WritableUtils.writeVInt(out, path.size());
                    for (MicroElement element : path) {
                        if (element instanceof FaunusVertex.MicroVertex)
                            out.writeChar('v');
                        else
                            out.writeChar('e');
                        WritableUtils.writeVLong(out, element.getId());
                    }
                }
            }
        }

        public static List<List<MicroElement>> readFields(final DataInput in) throws IOException {
            int pathsSize = WritableUtils.readVInt(in);
            if (pathsSize == 0)
                return new ArrayList<List<MicroElement>>();
            else {
                final List<List<MicroElement>> paths = new ArrayList<List<MicroElement>>(pathsSize);
                for (int i = 0; i < pathsSize; i++) {
                    int pathSize = WritableUtils.readVInt(in);
                    final List<MicroElement> path = new ArrayList<MicroElement>(pathSize);
                    for (int j = 0; j < pathSize; j++) {
                        char type = in.readChar();
                        if (type == 'v')
                            path.add(new FaunusVertex.MicroVertex(WritableUtils.readVLong(in)));
                        else
                            path.add(new FaunusEdge.MicroEdge(WritableUtils.readVLong(in)));
                    }
                    paths.add(path);
                }
                return paths;
            }
        }

    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusElement.class);
        }

        @Override
        public int compare(final byte[] element1, final int start1, final int length1, final byte[] element2, final int start2, final int length2) {
            try {
                return Long.valueOf(readVLong(element1, start1)).compareTo(readVLong(element2, start2));
            } catch (IOException e) {
                return -1;
            }
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            if (a instanceof FaunusElement && b instanceof FaunusElement)
                return ((Long) (((FaunusElement) a).getIdAsLong())).compareTo(((FaunusElement) b).getIdAsLong());
            else
                return super.compare(a, b);
        }
    }

    public static abstract class MicroElement {

        protected final long id;

        public MicroElement(final long id) {
            this.id = id;
        }

        public long getId() {
            return this.id;
        }

        public int hashCode() {
            return Long.valueOf(this.id).hashCode();
        }

        public boolean equals(final Object object) {
            return (object.getClass().equals(this.getClass()) && this.id == ((MicroElement) object).getId());
        }
    }
}
