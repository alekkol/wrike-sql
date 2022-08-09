package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.StreamReadCapability;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.async.NonBlockingInputFeeder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.JacksonFeatureSet;
import com.fasterxml.jackson.core.util.RequestPayload;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Objects;

public class LowerCaseKeyJsonParser extends JsonParser {
    private final JsonParser delegate;

    public LowerCaseKeyJsonParser(JsonParser delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public ObjectCodec getCodec() {
        return delegate.getCodec();
    }

    @Override
    public void setCodec(ObjectCodec oc) {
        delegate.setCodec(oc);
    }

    @Override
    public Object getInputSource() {
        return delegate.getInputSource();
    }

    @Override
    public void setRequestPayloadOnError(RequestPayload payload) {
        delegate.setRequestPayloadOnError(payload);
    }

    @Override
    public void setRequestPayloadOnError(byte[] payload, String charset) {
        delegate.setRequestPayloadOnError(payload, charset);
    }

    @Override
    public void setRequestPayloadOnError(String payload) {
        delegate.setRequestPayloadOnError(payload);
    }

    @Override
    public void setSchema(FormatSchema schema) {
        delegate.setSchema(schema);
    }

    @Override
    public FormatSchema getSchema() {
        return delegate.getSchema();
    }

    @Override
    public boolean canUseSchema(FormatSchema schema) {
        return delegate.canUseSchema(schema);
    }

    @Override
    public boolean requiresCustomCodec() {
        return delegate.requiresCustomCodec();
    }

    @Override
    public boolean canParseAsync() {
        return delegate.canParseAsync();
    }

    @Override
    public NonBlockingInputFeeder getNonBlockingInputFeeder() {
        return delegate.getNonBlockingInputFeeder();
    }

    @Override
    public JacksonFeatureSet<StreamReadCapability> getReadCapabilities() {
        return delegate.getReadCapabilities();
    }

    @Override
    public Version version() {
        return delegate.version();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public JsonStreamContext getParsingContext() {
        return delegate.getParsingContext();
    }

    @Override
    public JsonLocation currentLocation() {
        return delegate.currentLocation();
    }

    @Override
    public JsonLocation currentTokenLocation() {
        return delegate.currentTokenLocation();
    }

    @Override
    public JsonLocation getCurrentLocation() {
        return delegate.getCurrentLocation();
    }

    @Override
    public JsonLocation getTokenLocation() {
        return delegate.getTokenLocation();
    }

    @Override
    public Object currentValue() {
        return delegate.currentValue();
    }

    @Override
    public void assignCurrentValue(Object v) {
        delegate.assignCurrentValue(v);
    }

    @Override
    public Object getCurrentValue() {
        return delegate.getCurrentValue();
    }

    @Override
    public void setCurrentValue(Object v) {
        delegate.setCurrentValue(v);
    }

    @Override
    public int releaseBuffered(OutputStream out) throws IOException {
        return delegate.releaseBuffered(out);
    }

    @Override
    public int releaseBuffered(Writer w) throws IOException {
        return delegate.releaseBuffered(w);
    }

    @Override
    public JsonParser enable(Feature f) {
        return delegate.enable(f);
    }

    @Override
    public JsonParser disable(Feature f) {
        return delegate.disable(f);
    }

    @Override
    public JsonParser configure(Feature f, boolean state) {
        return delegate.configure(f, state);
    }

    @Override
    public boolean isEnabled(Feature f) {
        return delegate.isEnabled(f);
    }

    @Override
    public boolean isEnabled(StreamReadFeature f) {
        return delegate.isEnabled(f);
    }

    @Override
    public int getFeatureMask() {
        return delegate.getFeatureMask();
    }

    @Override
    @Deprecated
    public JsonParser setFeatureMask(int mask) {
        return delegate.setFeatureMask(mask);
    }

    @Override
    public JsonParser overrideStdFeatures(int values, int mask) {
        return delegate.overrideStdFeatures(values, mask);
    }

    @Override
    public int getFormatFeatures() {
        return delegate.getFormatFeatures();
    }

    @Override
    public JsonParser overrideFormatFeatures(int values, int mask) {
        return delegate.overrideFormatFeatures(values, mask);
    }

    @Override
    public JsonToken nextToken() throws IOException {
        return delegate.nextToken();
    }

    @Override
    public JsonToken nextValue() throws IOException {
        return delegate.nextValue();
    }

    @Override
    public boolean nextFieldName(SerializableString str) throws IOException {
        return delegate.nextFieldName(str);
    }

    @Override
    public String nextFieldName() throws IOException {
        String fieldName = delegate.nextFieldName();
        return delegate.currentToken() == JsonToken.FIELD_NAME
                ? fieldName.toLowerCase()
                : fieldName;
    }

    @Override
    public String nextTextValue() throws IOException {
        return delegate.nextTextValue();
    }

    @Override
    public int nextIntValue(int defaultValue) throws IOException {
        return delegate.nextIntValue(defaultValue);
    }

    @Override
    public long nextLongValue(long defaultValue) throws IOException {
        return delegate.nextLongValue(defaultValue);
    }

    @Override
    public Boolean nextBooleanValue() throws IOException {
        return delegate.nextBooleanValue();
    }

    @Override
    public JsonParser skipChildren() throws IOException {
        return delegate.skipChildren();
    }

    @Override
    public void finishToken() throws IOException {
        delegate.finishToken();
    }

    @Override
    public JsonToken currentToken() {
        return delegate.currentToken();
    }

    @Override
    public int currentTokenId() {
        return delegate.currentTokenId();
    }

    @Override
    public JsonToken getCurrentToken() {
        return delegate.getCurrentToken();
    }

    @Override
    @Deprecated
    public int getCurrentTokenId() {
        return delegate.getCurrentTokenId();
    }

    @Override
    public boolean hasCurrentToken() {
        return delegate.hasCurrentToken();
    }

    @Override
    public boolean hasTokenId(int id) {
        return delegate.hasTokenId(id);
    }

    @Override
    public boolean hasToken(JsonToken t) {
        return delegate.hasToken(t);
    }

    @Override
    public boolean isExpectedStartArrayToken() {
        return delegate.isExpectedStartArrayToken();
    }

    @Override
    public boolean isExpectedStartObjectToken() {
        return delegate.isExpectedStartObjectToken();
    }

    @Override
    public boolean isExpectedNumberIntToken() {
        return delegate.isExpectedNumberIntToken();
    }

    @Override
    public boolean isNaN() throws IOException {
        return delegate.isNaN();
    }

    @Override
    public void clearCurrentToken() {
        delegate.clearCurrentToken();
    }

    @Override
    public JsonToken getLastClearedToken() {
        return delegate.getLastClearedToken();
    }

    @Override
    public void overrideCurrentName(String name) {
        delegate.overrideCurrentName(name);
    }

    @Override
    public String getCurrentName() throws IOException {
        return delegate.getCurrentName();
    }

    @Override
    public String currentName() throws IOException {
        return delegate.currentName();
    }

    @Override
    public String getText() throws IOException {
        return delegate.getText();
    }

    @Override
    public int getText(Writer writer) throws IOException, UnsupportedOperationException {
        return delegate.getText(writer);
    }

    @Override
    public char[] getTextCharacters() throws IOException {
        return delegate.getTextCharacters();
    }

    @Override
    public int getTextLength() throws IOException {
        return delegate.getTextLength();
    }

    @Override
    public int getTextOffset() throws IOException {
        return delegate.getTextOffset();
    }

    @Override
    public boolean hasTextCharacters() {
        return delegate.hasTextCharacters();
    }

    @Override
    public Number getNumberValue() throws IOException {
        return delegate.getNumberValue();
    }

    @Override
    public Number getNumberValueExact() throws IOException {
        return delegate.getNumberValueExact();
    }

    @Override
    public NumberType getNumberType() throws IOException {
        return delegate.getNumberType();
    }

    @Override
    public byte getByteValue() throws IOException {
        return delegate.getByteValue();
    }

    @Override
    public short getShortValue() throws IOException {
        return delegate.getShortValue();
    }

    @Override
    public int getIntValue() throws IOException {
        return delegate.getIntValue();
    }

    @Override
    public long getLongValue() throws IOException {
        return delegate.getLongValue();
    }

    @Override
    public BigInteger getBigIntegerValue() throws IOException {
        return delegate.getBigIntegerValue();
    }

    @Override
    public float getFloatValue() throws IOException {
        return delegate.getFloatValue();
    }

    @Override
    public double getDoubleValue() throws IOException {
        return delegate.getDoubleValue();
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        return delegate.getDecimalValue();
    }

    @Override
    public boolean getBooleanValue() throws IOException {
        return delegate.getBooleanValue();
    }

    @Override
    public Object getEmbeddedObject() throws IOException {
        return delegate.getEmbeddedObject();
    }

    @Override
    public byte[] getBinaryValue(Base64Variant bv) throws IOException {
        return delegate.getBinaryValue(bv);
    }

    @Override
    public byte[] getBinaryValue() throws IOException {
        return delegate.getBinaryValue();
    }

    @Override
    public int readBinaryValue(OutputStream out) throws IOException {
        return delegate.readBinaryValue(out);
    }

    @Override
    public int readBinaryValue(Base64Variant bv, OutputStream out) throws IOException {
        return delegate.readBinaryValue(bv, out);
    }

    @Override
    public int getValueAsInt() throws IOException {
        return delegate.getValueAsInt();
    }

    @Override
    public int getValueAsInt(int def) throws IOException {
        return delegate.getValueAsInt(def);
    }

    @Override
    public long getValueAsLong() throws IOException {
        return delegate.getValueAsLong();
    }

    @Override
    public long getValueAsLong(long def) throws IOException {
        return delegate.getValueAsLong(def);
    }

    @Override
    public double getValueAsDouble() throws IOException {
        return delegate.getValueAsDouble();
    }

    @Override
    public double getValueAsDouble(double def) throws IOException {
        return delegate.getValueAsDouble(def);
    }

    @Override
    public boolean getValueAsBoolean() throws IOException {
        return delegate.getValueAsBoolean();
    }

    @Override
    public boolean getValueAsBoolean(boolean def) throws IOException {
        return delegate.getValueAsBoolean(def);
    }

    @Override
    public String getValueAsString() throws IOException {
        return delegate.getValueAsString();
    }

    @Override
    public String getValueAsString(String def) throws IOException {
        return delegate.getValueAsString(def);
    }

    @Override
    public boolean canReadObjectId() {
        return delegate.canReadObjectId();
    }

    @Override
    public boolean canReadTypeId() {
        return delegate.canReadTypeId();
    }

    @Override
    public Object getObjectId() throws IOException {
        return delegate.getObjectId();
    }

    @Override
    public Object getTypeId() throws IOException {
        return delegate.getTypeId();
    }

    @Override
    public <T> T readValueAs(Class<T> valueType) throws IOException {
        return delegate.readValueAs(valueType);
    }

    @Override
    public <T> T readValueAs(TypeReference<?> valueTypeRef) throws IOException {
        return delegate.readValueAs(valueTypeRef);
    }

    @Override
    public <T> Iterator<T> readValuesAs(Class<T> valueType) throws IOException {
        return delegate.readValuesAs(valueType);
    }

    @Override
    public <T> Iterator<T> readValuesAs(TypeReference<T> valueTypeRef) throws IOException {
        return delegate.readValuesAs(valueTypeRef);
    }

    @Override
    public <T extends TreeNode> T readValueAsTree() throws IOException {
        return delegate.readValueAsTree();
    }
}
