package com.shildon.hessian2json

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.dubbo.common.serialize.ObjectInput
import java.io.InputStream
import java.io.StringWriter
import java.lang.reflect.Type

/**
 * @author shildon
 * @since 1.0.0
 */
class Hessian2JsonObjectInput(
    inputStream: InputStream,
    private val jsonFactory: JsonFactory
) : ObjectInput {

    private val stringWriter = StringWriter()
    private val jsonGenerator = jsonFactory.createGenerator(stringWriter)
    private val hessian2JsonTranscoder = Hessian2JsonTranscoder(inputStream, stringWriter, jsonGenerator)

    override fun readAttachments(): Map<String, Any> {
        val objectMapper = ObjectMapper(jsonFactory)
        return objectMapper.readValue(this.readObject() as String, object : TypeReference<Map<String, Any>>() {})
    }

    override fun readBool(): Boolean {
        return hessian2JsonTranscoder.readBoolean()
    }

    override fun readByte(): Byte {
        return hessian2JsonTranscoder.readInt().toByte()
    }

    override fun readShort(): Short {
        return hessian2JsonTranscoder.readInt().toShort()
    }

    override fun readInt(): Int {
        return hessian2JsonTranscoder.readInt()
    }

    override fun readLong(): Long {
        return hessian2JsonTranscoder.readLong()
    }

    override fun readFloat(): Float {
        return hessian2JsonTranscoder.readDouble().toFloat()
    }

    override fun readDouble(): Double {
        return hessian2JsonTranscoder.readDouble()
    }

    override fun readUTF(): String? {
        return hessian2JsonTranscoder.readString()
    }

    override fun readBytes(): ByteArray? {
        return hessian2JsonTranscoder.readBytes()
    }

    override fun readObject(): Any? {
        stringWriter.buffer.setLength(0)
        hessian2JsonTranscoder.readMap()
        jsonGenerator.flush()
        return stringWriter.toString()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> readObject(cls: Class<T>?): T? {
        return this.readObject() as T?
    }

    override fun <T : Any?> readObject(cls: Class<T>?, type: Type?): T? {
        return this.readObject(cls)
    }

}
