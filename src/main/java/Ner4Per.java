import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ner4Per {

  public interface Iface {

    public String getpers(String title, String content, double weight) throws org.apache.thrift.TException;

  }

  public interface AsyncIface {

    public void getpers(String title, String content, double weight, org.apache.thrift.async.AsyncMethodCallback resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client extends org.apache.thrift.TServiceClient implements Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      super(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
      super(iprot, oprot);
    }

    public String getpers(String title, String content, double weight) throws org.apache.thrift.TException
    {
      send_getpers(title, content, weight);
      return recv_getpers();
    }

    public void send_getpers(String title, String content, double weight) throws org.apache.thrift.TException
    {
      getpers_args args = new getpers_args();
      args.setTitle(title);
      args.setContent(content);
      args.setWeight(weight);
      sendBase("getpers", args);
    }

    public String recv_getpers() throws org.apache.thrift.TException
    {
      getpers_result result = new getpers_result();
      receiveBase(result, "getpers");
      if (result.isSetSuccess()) {
        return result.success;
      }
      throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, "getpers failed: unknown result");
    }

  }
  public static class AsyncClient extends org.apache.thrift.async.TAsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void getpers(String title, String content, double weight, org.apache.thrift.async.AsyncMethodCallback resultHandler) throws org.apache.thrift.TException {
      checkReady();
      getpers_call method_call = new getpers_call(title, content, weight, resultHandler, this, ___protocolFactory, ___transport);
      this.___currentMethod = method_call;
      ___manager.call(method_call);
    }

    public static class getpers_call extends org.apache.thrift.async.TAsyncMethodCall {
      private String title;
      private String content;
      private double weight;
      public getpers_call(String title, String content, double weight, org.apache.thrift.async.AsyncMethodCallback resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, false);
        this.title = title;
        this.content = content;
        this.weight = weight;
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("getpers", org.apache.thrift.protocol.TMessageType.CALL, 0));
        getpers_args args = new getpers_args();
        args.setTitle(title);
        args.setContent(content);
        args.setWeight(weight);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public String getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return (new Client(prot)).recv_getpers();
      }
    }

  }

  public static class Processor<I extends Iface> extends org.apache.thrift.TBaseProcessor<I> implements org.apache.thrift.TProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());
    public Processor(I iface) {
      super(iface, getProcessMap(new HashMap<String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>>()));
    }

    protected Processor(I iface, Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends Iface> Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> getProcessMap(Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      processMap.put("getpers", new getpers());
      return processMap;
    }

    public static class getpers<I extends Iface> extends org.apache.thrift.ProcessFunction<I, getpers_args> {
      public getpers() {
        super("getpers");
      }

      public getpers_args getEmptyArgsInstance() {
        return new getpers_args();
      }

      protected boolean isOneway() {
        return false;
      }

      public getpers_result getResult(I iface, getpers_args args) throws org.apache.thrift.TException {
        getpers_result result = new getpers_result();
        result.success = iface.getpers(args.title, args.content, args.weight);
        return result;
      }
    }

  }

  public static class AsyncProcessor<I extends AsyncIface> extends org.apache.thrift.TBaseAsyncProcessor<I> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProcessor.class.getName());
    public AsyncProcessor(I iface) {
      super(iface, getProcessMap(new HashMap<String, org.apache.thrift.AsyncProcessFunction<I, ? extends org.apache.thrift.TBase, ?>>()));
    }

    protected AsyncProcessor(I iface, Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends AsyncIface> Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase,?>> getProcessMap(Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      processMap.put("getpers", new getpers());
      return processMap;
    }

    public static class getpers<I extends AsyncIface> extends org.apache.thrift.AsyncProcessFunction<I, getpers_args, String> {
      public getpers() {
        super("getpers");
      }

      public getpers_args getEmptyArgsInstance() {
        return new getpers_args();
      }

      public AsyncMethodCallback<String> getResultHandler(final AsyncFrameBuffer fb, final int seqid) {
        final org.apache.thrift.AsyncProcessFunction fcall = this;
        return new AsyncMethodCallback<String>() { 
          public void onComplete(String o) {
            getpers_result result = new getpers_result();
            result.success = o;
            try {
              fcall.sendResponse(fb,result, org.apache.thrift.protocol.TMessageType.REPLY,seqid);
              return;
            } catch (Exception e) {
              LOGGER.error("Exception writing to internal frame buffer", e);
            }
            fb.close();
          }
          public void onError(Exception e) {
            byte msgType = org.apache.thrift.protocol.TMessageType.REPLY;
            org.apache.thrift.TBase msg;
            getpers_result result = new getpers_result();
            {
              msgType = org.apache.thrift.protocol.TMessageType.EXCEPTION;
              msg = (org.apache.thrift.TBase)new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.INTERNAL_ERROR, e.getMessage());
            }
            try {
              fcall.sendResponse(fb,msg,msgType,seqid);
              return;
            } catch (Exception ex) {
              LOGGER.error("Exception writing to internal frame buffer", ex);
            }
            fb.close();
          }
        };
      }

      protected boolean isOneway() {
        return false;
      }

      public void start(I iface, getpers_args args, org.apache.thrift.async.AsyncMethodCallback<String> resultHandler) throws TException {
        iface.getpers(args.title, args.content, args.weight,resultHandler);
      }
    }

  }

  public static class getpers_args implements org.apache.thrift.TBase<getpers_args, getpers_args._Fields>, java.io.Serializable, Cloneable, Comparable<getpers_args>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("getpers_args");

    private static final org.apache.thrift.protocol.TField TITLE_FIELD_DESC = new org.apache.thrift.protocol.TField("title", org.apache.thrift.protocol.TType.STRING, (short)1);
    private static final org.apache.thrift.protocol.TField CONTENT_FIELD_DESC = new org.apache.thrift.protocol.TField("content", org.apache.thrift.protocol.TType.STRING, (short)2);
    private static final org.apache.thrift.protocol.TField WEIGHT_FIELD_DESC = new org.apache.thrift.protocol.TField("weight", org.apache.thrift.protocol.TType.DOUBLE, (short)3);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
      schemes.put(StandardScheme.class, new getpers_argsStandardSchemeFactory());
      schemes.put(TupleScheme.class, new getpers_argsTupleSchemeFactory());
    }

    public String title; // required
    public String content; // required
    public double weight; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      TITLE((short)1, "title"),
      CONTENT((short)2, "content"),
      WEIGHT((short)3, "weight");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 1: // TITLE
            return TITLE;
          case 2: // CONTENT
            return CONTENT;
          case 3: // WEIGHT
            return WEIGHT;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    private static final int __WEIGHT_ISSET_ID = 0;
    private byte __isset_bitfield = 0;
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.TITLE, new org.apache.thrift.meta_data.FieldMetaData("title", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
      tmpMap.put(_Fields.CONTENT, new org.apache.thrift.meta_data.FieldMetaData("content", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
      tmpMap.put(_Fields.WEIGHT, new org.apache.thrift.meta_data.FieldMetaData("weight", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(getpers_args.class, metaDataMap);
    }

    public getpers_args() {
    }

    public getpers_args(
      String title,
      String content,
      double weight)
    {
      this();
      this.title = title;
      this.content = content;
      this.weight = weight;
      setWeightIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getpers_args(getpers_args other) {
      __isset_bitfield = other.__isset_bitfield;
      if (other.isSetTitle()) {
        this.title = other.title;
      }
      if (other.isSetContent()) {
        this.content = other.content;
      }
      this.weight = other.weight;
    }

    public getpers_args deepCopy() {
      return new getpers_args(this);
    }

    @Override
    public void clear() {
      this.title = null;
      this.content = null;
      setWeightIsSet(false);
      this.weight = 0.0;
    }

    public String getTitle() {
      return this.title;
    }

    public getpers_args setTitle(String title) {
      this.title = title;
      return this;
    }

    public void unsetTitle() {
      this.title = null;
    }

    /** Returns true if field title is set (has been assigned a value) and false otherwise */
    public boolean isSetTitle() {
      return this.title != null;
    }

    public void setTitleIsSet(boolean value) {
      if (!value) {
        this.title = null;
      }
    }

    public String getContent() {
      return this.content;
    }

    public getpers_args setContent(String content) {
      this.content = content;
      return this;
    }

    public void unsetContent() {
      this.content = null;
    }

    /** Returns true if field content is set (has been assigned a value) and false otherwise */
    public boolean isSetContent() {
      return this.content != null;
    }

    public void setContentIsSet(boolean value) {
      if (!value) {
        this.content = null;
      }
    }

    public double getWeight() {
      return this.weight;
    }

    public getpers_args setWeight(double weight) {
      this.weight = weight;
      setWeightIsSet(true);
      return this;
    }

    public void unsetWeight() {
      __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __WEIGHT_ISSET_ID);
    }

    /** Returns true if field weight is set (has been assigned a value) and false otherwise */
    public boolean isSetWeight() {
      return EncodingUtils.testBit(__isset_bitfield, __WEIGHT_ISSET_ID);
    }

    public void setWeightIsSet(boolean value) {
      __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __WEIGHT_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case TITLE:
        if (value == null) {
          unsetTitle();
        } else {
          setTitle((String)value);
        }
        break;

      case CONTENT:
        if (value == null) {
          unsetContent();
        } else {
          setContent((String)value);
        }
        break;

      case WEIGHT:
        if (value == null) {
          unsetWeight();
        } else {
          setWeight((Double)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case TITLE:
        return getTitle();

      case CONTENT:
        return getContent();

      case WEIGHT:
        return Double.valueOf(getWeight());

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case TITLE:
        return isSetTitle();
      case CONTENT:
        return isSetContent();
      case WEIGHT:
        return isSetWeight();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getpers_args)
        return this.equals((getpers_args)that);
      return false;
    }

    public boolean equals(getpers_args that) {
      if (that == null)
        return false;

      boolean this_present_title = true && this.isSetTitle();
      boolean that_present_title = true && that.isSetTitle();
      if (this_present_title || that_present_title) {
        if (!(this_present_title && that_present_title))
          return false;
        if (!this.title.equals(that.title))
          return false;
      }

      boolean this_present_content = true && this.isSetContent();
      boolean that_present_content = true && that.isSetContent();
      if (this_present_content || that_present_content) {
        if (!(this_present_content && that_present_content))
          return false;
        if (!this.content.equals(that.content))
          return false;
      }

      boolean this_present_weight = true;
      boolean that_present_weight = true;
      if (this_present_weight || that_present_weight) {
        if (!(this_present_weight && that_present_weight))
          return false;
        if (this.weight != that.weight)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public int compareTo(getpers_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = Boolean.valueOf(isSetTitle()).compareTo(other.isSetTitle());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetTitle()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.title, other.title);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      lastComparison = Boolean.valueOf(isSetContent()).compareTo(other.isSetContent());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetContent()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.content, other.content);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      lastComparison = Boolean.valueOf(isSetWeight()).compareTo(other.isSetWeight());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetWeight()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.weight, other.weight);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getpers_args(");
      boolean first = true;

      sb.append("title:");
      if (this.title == null) {
        sb.append("null");
      } else {
        sb.append(this.title);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("content:");
      if (this.content == null) {
        sb.append("null");
      } else {
        sb.append(this.content);
      }
      first = false;
      if (!first) sb.append(", ");
      sb.append("weight:");
      sb.append(this.weight);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
        __isset_bitfield = 0;
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class getpers_argsStandardSchemeFactory implements SchemeFactory {
      public getpers_argsStandardScheme getScheme() {
        return new getpers_argsStandardScheme();
      }
    }

    private static class getpers_argsStandardScheme extends StandardScheme<getpers_args> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, getpers_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 1: // TITLE
              if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                struct.title = iprot.readString();
                struct.setTitleIsSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            case 2: // CONTENT
              if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                struct.content = iprot.readString();
                struct.setContentIsSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            case 3: // WEIGHT
              if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
                struct.weight = iprot.readDouble();
                struct.setWeightIsSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();

        // check for required fields of primitive type, which can't be checked in the validate method
        struct.validate();
      }

      public void write(org.apache.thrift.protocol.TProtocol oprot, getpers_args struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.title != null) {
          oprot.writeFieldBegin(TITLE_FIELD_DESC);
          oprot.writeString(struct.title);
          oprot.writeFieldEnd();
        }
        if (struct.content != null) {
          oprot.writeFieldBegin(CONTENT_FIELD_DESC);
          oprot.writeString(struct.content);
          oprot.writeFieldEnd();
        }
        oprot.writeFieldBegin(WEIGHT_FIELD_DESC);
        oprot.writeDouble(struct.weight);
        oprot.writeFieldEnd();
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class getpers_argsTupleSchemeFactory implements SchemeFactory {
      public getpers_argsTupleScheme getScheme() {
        return new getpers_argsTupleScheme();
      }
    }

    private static class getpers_argsTupleScheme extends TupleScheme<getpers_args> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, getpers_args struct) throws org.apache.thrift.TException {
        TTupleProtocol oprot = (TTupleProtocol) prot;
        BitSet optionals = new BitSet();
        if (struct.isSetTitle()) {
          optionals.set(0);
        }
        if (struct.isSetContent()) {
          optionals.set(1);
        }
        if (struct.isSetWeight()) {
          optionals.set(2);
        }
        oprot.writeBitSet(optionals, 3);
        if (struct.isSetTitle()) {
          oprot.writeString(struct.title);
        }
        if (struct.isSetContent()) {
          oprot.writeString(struct.content);
        }
        if (struct.isSetWeight()) {
          oprot.writeDouble(struct.weight);
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, getpers_args struct) throws org.apache.thrift.TException {
        TTupleProtocol iprot = (TTupleProtocol) prot;
        BitSet incoming = iprot.readBitSet(3);
        if (incoming.get(0)) {
          struct.title = iprot.readString();
          struct.setTitleIsSet(true);
        }
        if (incoming.get(1)) {
          struct.content = iprot.readString();
          struct.setContentIsSet(true);
        }
        if (incoming.get(2)) {
          struct.weight = iprot.readDouble();
          struct.setWeightIsSet(true);
        }
      }
    }

  }

  public static class getpers_result implements org.apache.thrift.TBase<getpers_result, getpers_result._Fields>, java.io.Serializable, Cloneable, Comparable<getpers_result>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("getpers_result");

    private static final org.apache.thrift.protocol.TField SUCCESS_FIELD_DESC = new org.apache.thrift.protocol.TField("success", org.apache.thrift.protocol.TType.STRING, (short)0);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
      schemes.put(StandardScheme.class, new getpers_resultStandardSchemeFactory());
      schemes.put(TupleScheme.class, new getpers_resultTupleSchemeFactory());
    }

    public String success; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      SUCCESS((short)0, "success");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 0: // SUCCESS
            return SUCCESS;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
      Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.SUCCESS, new org.apache.thrift.meta_data.FieldMetaData("success", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(getpers_result.class, metaDataMap);
    }

    public getpers_result() {
    }

    public getpers_result(
      String success)
    {
      this();
      this.success = success;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getpers_result(getpers_result other) {
      if (other.isSetSuccess()) {
        this.success = other.success;
      }
    }

    public getpers_result deepCopy() {
      return new getpers_result(this);
    }

    @Override
    public void clear() {
      this.success = null;
    }

    public String getSuccess() {
      return this.success;
    }

    public getpers_result setSuccess(String success) {
      this.success = success;
      return this;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    /** Returns true if field success is set (has been assigned a value) and false otherwise */
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((String)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case SUCCESS:
        return getSuccess();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case SUCCESS:
        return isSetSuccess();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getpers_result)
        return this.equals((getpers_result)that);
      return false;
    }

    public boolean equals(getpers_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public int compareTo(getpers_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = Boolean.valueOf(isSetSuccess()).compareTo(other.isSetSuccess());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSuccess()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.success, other.success);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
      schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
      schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
      }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("getpers_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
      try {
        write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
      try {
        read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
      } catch (org.apache.thrift.TException te) {
        throw new java.io.IOException(te);
      }
    }

    private static class getpers_resultStandardSchemeFactory implements SchemeFactory {
      public getpers_resultStandardScheme getScheme() {
        return new getpers_resultStandardScheme();
      }
    }

    private static class getpers_resultStandardScheme extends StandardScheme<getpers_result> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, getpers_result struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 0: // SUCCESS
              if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                struct.success = iprot.readString();
                struct.setSuccessIsSet(true);
              } else { 
                org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
              }
              break;
            default:
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();

        // check for required fields of primitive type, which can't be checked in the validate method
        struct.validate();
      }

      public void write(org.apache.thrift.protocol.TProtocol oprot, getpers_result struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.success != null) {
          oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
          oprot.writeString(struct.success);
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class getpers_resultTupleSchemeFactory implements SchemeFactory {
      public getpers_resultTupleScheme getScheme() {
        return new getpers_resultTupleScheme();
      }
    }

    private static class getpers_resultTupleScheme extends TupleScheme<getpers_result> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, getpers_result struct) throws org.apache.thrift.TException {
        TTupleProtocol oprot = (TTupleProtocol) prot;
        BitSet optionals = new BitSet();
        if (struct.isSetSuccess()) {
          optionals.set(0);
        }
        oprot.writeBitSet(optionals, 1);
        if (struct.isSetSuccess()) {
          oprot.writeString(struct.success);
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, getpers_result struct) throws org.apache.thrift.TException {
        TTupleProtocol iprot = (TTupleProtocol) prot;
        BitSet incoming = iprot.readBitSet(1);
        if (incoming.get(0)) {
          struct.success = iprot.readString();
          struct.setSuccessIsSet(true);
        }
      }
    }

  }

}
