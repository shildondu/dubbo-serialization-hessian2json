package com.shildon.hessian2json

import com.fasterxml.jackson.core.JsonGenerator
import com.shildon.hessian2json.exception.Hessian2UnexpectedException
import java.io.InputStream
import java.io.Writer
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.util.ArrayList
import java.util.HashMap

/**
 * @author shildon
 * @since 1.0.0
 */
class Hessian2JsonTranscoder(
    private val inputStream: InputStream,
    private val writer: Writer,
    private val jsonGenerator: JsonGenerator
) {

    companion object {
        private const val BUFFER_SIZE = 256

        const val JSON_SKIP = 0
        const val JSON_FIELD_NAME = 1
        const val JSON_FIELD_VALUE = 2
    }

    private val buffer = ByteArray(BUFFER_SIZE)
    private var offset = 0
    private var length = 0

    private var isLastChunk = false
    private var chunkLength = 0
    private val stringBuilder = StringBuilder()

    private val refs = mutableListOf<Any>()
    private val classDefinitions = mutableListOf<ClassDefinition>()

    private fun currentJsonString(): String {
        jsonGenerator.flush()
        return writer.toString()
    }

    private fun codeName(char: Int): String {
        return if (char < 0) {
            "end of file"
        } else {
            "0x" + Integer.toHexString(char and 0xff) + " (" + char.toChar() + ")"
        }
    }

    /**
     * read a byte
     */
    private fun readBuffer(): Boolean {
        val buffer = this.buffer
        var offset = this.offset
        val length = this.length

        offset = if (offset < length) {
            System.arraycopy(buffer, offset, buffer, 0, length - offset)
            length - offset
        } else {
            0
        }

        val newReadLength = inputStream.read(buffer, offset, BUFFER_SIZE - offset)

        if (newReadLength <= 0) {
            this.length = offset
            this.offset = 0

            return offset > 0
        }

        this.length = offset + newReadLength
        this.offset = 0

        return true
    }

    /**
     * read a byte
     */
    private fun read(): Int =
        if (this.offset >= this.length && !readBuffer()) {
            -1
        } else {
            this.buffer[this.offset++].toInt() and 0xff
        }

    /**
     * Parses a 32-bit integer value from the stream.
     * b32 b24 b16 b8
     */
    private fun parseInt(): Int {
        val offset = this.offset
        val size = 3
        return if (offset + size < this.length) {
            val buffer = this.buffer
            val b32 = buffer[offset].toInt() and 0xff
            val b24 = buffer[offset + 1].toInt() and 0xff
            val b16 = buffer[offset + 2].toInt() and 0xff
            val b8 = buffer[offset + 3].toInt() and 0xff
            this.offset = offset + 4
            (b32 shl 24) + (b24 shl 16) + (b16 shl 8) + b8
        } else {
            val b32 = read()
            val b24 = read()
            val b16 = read()
            val b8 = read()
            (b32 shl 24) + (b24 shl 16) + (b16 shl 8) + b8
        }
    }

    /**
     * Parses a 64-bit long value from the stream.
     * b64 b56 b48 b40 b32 b24 b16 b8
     */
    private fun parseLong(): Long {
        val b64 = read().toLong()
        val b56 = read().toLong()
        val b48 = read().toLong()
        val b40 = read().toLong()
        val b32 = read().toLong()
        val b24 = read().toLong()
        val b16 = read().toLong()
        val b8 = read().toLong()

        return ((b64 shl 56)
                + (b56 shl 48)
                + (b48 shl 40)
                + (b40 shl 32)
                + (b32 shl 24)
                + (b24 shl 16)
                + (b16 shl 8)
                + b8)
    }

    /**
     * Parses a 64-bit double value from the stream.
     * b64 b56 b48 b40 b32 b24 b16 b8
     */
    private fun parseDouble(): Double {
        val bits = parseLong()
        return java.lang.Double.longBitsToDouble(bits)
    }

    /**
     * parses a string value from the stream.
     */
    private fun parseString(stringBuilder: StringBuilder) {
        while (true) {
            if (chunkLength <= 0) {
                if (!parseChunkLength()) {
                    return
                }
            }
            var length = chunkLength
            chunkLength = 0
            while (length-- > 0) {
                stringBuilder.append(parseUtf8Char().toChar())
            }
        }
    }

    /**
     * Parses a single UTF8 character.
     */
    private fun parseUtf8Char(): Int {
        val ch = if (offset < length) buffer[offset++].toInt() and 0xff else read()
        return when {
            ch < 0x80 -> {
                ch
            }
            ch and 0xe0 == 0xc0 -> {
                val ch1 = read()
                (ch and 0x1f shl 6) + (ch1 and 0x3f)
            }
            ch and 0xf0 == 0xe0 -> {
                val ch1 = read()
                val ch2 = read()
                (ch and 0x0f shl 12) + (ch1 and 0x3f shl 6) + (ch2 and 0x3f)
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "utf-8", codeName(ch))
            }
        }
    }

    /**
     * parse chunk length
     */
    private fun parseChunkLength(): Boolean {
        if (isLastChunk) {
            return false
        }
        when (val tag: Int = this.readTag()) {
            0x52 -> {
                isLastChunk = false
                chunkLength = (read() shl 8) + read()
            }
            'S'.toInt() -> {
                isLastChunk = true
                chunkLength = (read() shl 8) + read()
            }
            in 0x00..0x1f -> {
                isLastChunk = true
                chunkLength = tag
            }
            in 0x30..0x33 -> {
                isLastChunk = true
                chunkLength = (tag - 0x30) * 256 + read()
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "string", codeName(tag))
            }
        }
        return true
    }

    /**
     * read next byte
     */
    private fun readTag(): Int =
        if (offset < length) buffer[offset++].toInt() and 0xff else read()

    /**
     * forward the offset to next position but do not increase.
     */
    private fun forward(): Int {
        val code: Int
        if (offset < length) {
            code = buffer[offset].toInt() and 0xff
        } else {
            code = read()
            if (code >= 0) {
                offset--
            }
        }
        return code
    }

    /**
     * return true if this is a null value.
     */
    private fun isNull() = forward() == 'N'.toInt()

    /**
     * return true if this is a boolean value.
     */
    private fun isBoolean() =
        with(forward()) {
            this == 'T'.toInt() || this == 'F'.toInt()
        }

    /**
     * return true if this is a int value.
     */
    private fun isInt() =
        with(forward()) {
            this == 'I'.toInt()
                    || (this in 0x80..0xbf)
                    || (this in 0xc0..0xcf)
                    || (this in 0xd0..0xd7)
        }

    /**
     * return true if this is a long value.
     */
    private fun isLong() =
        with(forward()) {
            this == 'L'.toInt()
                    || (this in 0xd8..0xef)
                    || (this in 0xf0..0xff)
                    || (this in 0x38..0x3f)
                    || this == 0x59
        }

    /**
     * return true if this is a double value.
     */
    private fun isDouble() =
        with(forward()) {
            this == 'D'.toInt() || (this in 0x5b..0x5f)
        }

    /**
     * return true if this is a list value.
     */
    private fun isList() =
        with(forward()) {
            this == 0x55
                    || this == 'V'.toInt()
                    || this == 0x57
                    || this == 0x58
                    || (this in 0x70..0x77)
                    || (this in 0x78..0x7f)
        }

    /**
     * return true if this is a map value.
     */
    private fun isMap() =
        with(forward()) {
            this == 'M'.toInt() || this == 'H'.toInt()
        }

    /**
     * return true if this is a object value.
     */
    private fun isObject() =
        with(forward()) {
            this == 'O'.toInt() || (this in 0x60..0x6f)
        }

    /**
     * return true if this is a ref.
     */
    private fun isRef() = forward() == 0x51

    /**
     * return true if this is a string value.
     */
    private fun isString() =
        with(forward()) {
            this == 0x52
                    || this == 'S'.toInt()
                    || (this in 0x00..0x1f)
                    || (this in 0x30..0x34)
        }

    /**
     * return true if this is a date value.
     */
    private fun isDate() =
        with(forward()) {
            this == 0x4a || this == 0x4b
        }

    /**
     * return true if this is a class definition.
     */
    private fun isClassDefinition() = forward() == 'C'.toInt()

    /**
     * return true if this is the end of a list or a map.
     */
    private fun isEnd() =
        with(forward()) {
            this < 0 || this == 'Z'.toInt()
        }

    /**
     * read the end byte.
     */
    fun readEnd() {
        val code = if (offset < length) buffer[offset++].toInt() and 0xff else read()
        if (code != 'Z'.toInt()) {
            throw Hessian2UnexpectedException(currentJsonString(), "Z", codeName(code))
        }
    }

    /**
     * # null value
     * null ::= 'N'
     */
    fun readNull(flag: Int = JSON_SKIP) {
        val tag = readTag()
        if (tag == 'N'.toInt()) {
            if (flag == JSON_FIELD_NAME) {
                jsonGenerator.writeFieldName("null")
            } else if (flag == JSON_FIELD_VALUE) {
                jsonGenerator.writeNull()
            }
        } else {
            throw Hessian2UnexpectedException(currentJsonString(), "null", codeName(tag))
        }
    }

    /**
     * # boolean true/false
     * boolean ::= 'T'
     * boolean ::= 'F'
     */
    fun readBoolean(flag: Int = JSON_SKIP): Boolean {
        val value = when (val tag = readTag()) {
            'T'.toInt() -> true
            'F'.toInt() -> false
            else -> throw Hessian2UnexpectedException(currentJsonString(), "boolean", codeName(tag))
        }
        if (flag == JSON_FIELD_NAME) {
            jsonGenerator.writeFieldName(value.toString())
        } else if (flag == JSON_FIELD_VALUE) {
            jsonGenerator.writeBoolean(value)
        }
        return value
    }

    /**
     * # 32-bit signed integer
     * int ::= 'I' b3 b2 b1 b0
     * int ::= [x80-xbf]             # -x10 to x3f
     * int ::= [xc0-xcf] b0          # -x800 to x7ff
     * int ::= [xd0-xd7] b1 b0       # -x40000 to x3ffff
     */
    fun readInt(flag: Int = JSON_SKIP): Int {
        val value = when (val tag = readTag()) {
            'I'.toInt() -> parseInt()
            in 0x80..0xbf -> tag - 0x90
            in 0xc0..0xcf -> (tag - 0xc8 shl 8) + read()
            in 0xd0..0xd7 -> ((tag - 0xd4 shl 16) + 256 * read() + read())
            else -> throw Hessian2UnexpectedException(currentJsonString(), "int", codeName(tag))
        }
        if (flag == JSON_FIELD_NAME) {
            jsonGenerator.writeFieldName(value.toString())
        } else if (flag == JSON_FIELD_VALUE) {
            jsonGenerator.writeNumber(value)
        }
        return value
    }

    /**
     * # 64-bit signed long integer
     * long ::= 'L' b7 b6 b5 b4 b3 b2 b1 b0
     * long ::= [xd8-xef]             # -x08 to x0f
     * long ::= [xf0-xff] b0          # -x800 to x7ff
     * long ::= [x38-x3f] b1 b0       # -x40000 to x3ffff
     * long ::= x59 b3 b2 b1 b0       # 32-bit integer cast to long
     */
    fun readLong(flag: Int = JSON_SKIP): Long {
        val value = when (val tag = readTag()) {
            'L'.toInt() -> parseLong()
            in 0xd8..0xef -> tag - 0xe0.toLong()
            in 0xf0..0xff -> (tag - 0xf8 shl 8) + read().toLong()
            in 0x38..0x3f -> (tag - 0x3c shl 16) + 256 * read() + read().toLong()
            0x59 -> parseInt().toLong()
            else -> throw Hessian2UnexpectedException(currentJsonString(), "long", codeName(tag))
        }
        if (flag == JSON_FIELD_NAME) {
            jsonGenerator.writeFieldName(value.toString())
        } else if (flag == JSON_FIELD_VALUE) {
            jsonGenerator.writeNumber(value)
        }
        return value
    }

    /**
     * # 64-bit IEEE double
     * double ::= 'D' b7 b6 b5 b4 b3 b2 b1 b0
     * double ::= x5b                   # 0.0
     * double ::= x5c                   # 1.0
     * double ::= x5d b0                # byte cast to double (-128.0 to 127.0)
     * double ::= x5e b1 b0             # short cast to double
     * double ::= x5f b3 b2 b1 b0       # 32-bit float cast to double
     */
    fun readDouble(flag: Int = JSON_SKIP): Double {
        val value = when (val tag = readTag()) {
            'D'.toInt() -> parseDouble()
            0x5b -> 0.0
            0x5c -> 1.0
            0x5d -> read().toByte().toDouble()
            0x5e -> (256 * read() + read()).toShort().toDouble()
            0x5f -> 0.001 * parseInt()
            else -> throw Hessian2UnexpectedException(currentJsonString(), "double", codeName(tag))
        }
        if (flag == JSON_FIELD_NAME) {
            jsonGenerator.writeFieldName(value.toString())
        } else if (flag == JSON_FIELD_VALUE) {
            jsonGenerator.writeNumber(value)
        }
        return value
    }

    /**
     * # UTF-8 encoded character string split into 64k chunks
     * string ::= x52 b1 b0 <utf8-data> string  # non-final chunk
     * string ::= 'S' b1 b0 <utf8-data>         # string of length 0-65535
     * string ::= [x00-x1f] <utf8-data>         # string of length 0-31
     * string ::= [x30-x34] <utf8-data>         # string of length 0-1023
     */
    fun readString(flag: Int = JSON_SKIP): String {
        val value = when (val tag = readTag()) {
            0x52, 'S'.toInt() -> {
                isLastChunk = tag == 'S'.toInt()
                chunkLength = (read() shl 8) + read()
                stringBuilder.setLength(0)
                parseString(stringBuilder)
                stringBuilder.toString()
            }
            in 0x00..0x1f -> {
                isLastChunk = true
                chunkLength = tag
                stringBuilder.setLength(0)
                parseString(stringBuilder)
                stringBuilder.toString()
            }
            in 0x30..0x33 -> {
                isLastChunk = true
                chunkLength = (tag - 0x30) * 256 + read()
                stringBuilder.setLength(0)
                parseString(stringBuilder)
                stringBuilder.toString()
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "string", codeName(tag))
            }
        }
        if (flag == JSON_FIELD_NAME) {
            jsonGenerator.writeFieldName(value)
        } else if (flag == JSON_FIELD_VALUE) {
            jsonGenerator.writeString(value)
        }
        return value
    }

    /**
     * # map/list types for OO languages
     * type ::= string                        # type name
     * type ::= int                           # type reference
     */
    fun readType() {
        when {
            isString() -> {
                this.readString(JSON_SKIP)
            }
            isInt() -> {
                readInt(JSON_SKIP)
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "type", codeName(forward()))
            }
        }
    }

    /**
     * # list/vector
     * list ::= x55 type value* 'Z'   # variable-length list
     * list ::= 'V' type int value*   # fixed-length list
     * list ::= x57 value* 'Z'        # variable-length untyped list
     * list ::= x58 int value*        # fixed-length untyped list
     * list ::= [x70-77] type value*  # fixed-length typed list
     * list ::= [x78-7f] value*       # fixed-length untyped list
     */
    fun readList(): List<Any?> {
        val tag = readTag()
        val list = mutableListOf<Any?>()
        refs.add(list)
        when (tag) {
            0x55 -> {
                this.readType()
                jsonGenerator.writeStartArray()
                while (!isEnd()) {
                    list.add(readValue(JSON_FIELD_VALUE))
                }
                readEnd()
                jsonGenerator.writeEndArray()
            }
            'V'.toInt() -> {
                this.readType()
                val size = readInt(JSON_SKIP)
                jsonGenerator.writeStartArray()
                for (i in 0 until size) {
                    list.add(readValue(JSON_FIELD_VALUE))
                }
                jsonGenerator.writeEndArray()
            }
            0x57 -> {
                jsonGenerator.writeStartArray()
                while (!isEnd()) {
                    list.add(readValue(JSON_FIELD_VALUE))
                }
                readEnd()
                jsonGenerator.writeEndArray()
            }
            0x58 -> {
                val size = readInt(JSON_SKIP)
                jsonGenerator.writeStartArray()
                for (i in 0 until size) {
                    list.add(readValue(JSON_FIELD_VALUE))
                }
                jsonGenerator.writeEndArray()
            }
            in 0x70..0x77 -> {
                this.readType()
                val size = tag - 0x70
                jsonGenerator.writeStartArray()
                for (i in 0 until size) {
                    list.add(readValue(JSON_FIELD_VALUE))
                }
                jsonGenerator.writeEndArray()
            }
            in 0x78..0x7f -> {
                val size = tag - 0x78
                jsonGenerator.writeStartArray()
                for (i in 0 until size) {
                    list.add(readValue(JSON_FIELD_VALUE))
                }
                jsonGenerator.writeEndArray()
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "list", codeName(tag))
            }
        }
        return list
    }

    /**
     * # time in UTC encoded as 64-bit long milliseconds since epoch
     * date ::= x4a b7 b6 b5 b4 b3 b2 b1 b0
     * date ::= x4b b3 b2 b1 b0       # minutes since epoch
     */
    fun readDate(flag: Int = JSON_SKIP): Long {
        val value = when (val tag = readTag()) {
            0x4a -> parseLong()
            0x4b -> parseInt() * 60000L
            else -> throw Hessian2UnexpectedException(currentJsonString(), "date", codeName(tag))
        }
        if (flag == JSON_FIELD_NAME) {
            jsonGenerator.writeFieldName(value.toString())
        } else if (flag == JSON_FIELD_VALUE) {
            jsonGenerator.writeNumber(value)
        }
        return value
    }

    /**
     * # definition for an object (compact map)
     * class-def  ::= 'C' string int string*
     */
    fun readClassDefinition(): ClassDefinition {
        val tag = readTag()
        return if (tag == 'C'.toInt()) {
            val type = readString(JSON_SKIP)
            val fieldSize = readInt(JSON_SKIP)
            val fieldNames: MutableList<String> = ArrayList()
            for (i in 0 until fieldSize) {
                fieldNames.add(readString(JSON_SKIP))
            }
            val classDefinition = ClassDefinition(type, fieldNames)
            classDefinitions.add(classDefinition)
            classDefinition
        } else {
            throw Hessian2UnexpectedException(currentJsonString(), "class-def", codeName(tag))
        }
    }

    /**
     * # Object instance
     * object ::= 'O' int value*
     * object ::= [x60-x6f] value*
     * TODO
     */
    fun readObject(): Any {
        return when (val tag = readTag()) {
            'O'.toInt() -> {
                // in generic invoking, pojo will be translated to map
                throw Hessian2UnexpectedException(currentJsonString(), "should not be 'O'", codeName(tag))
            }
            in 0x60..0x6f -> {
                val index = tag - 0x60
                val (type) = classDefinitions[index]
                // specially processing for big decimal and sql time
                when {
                    BigDecimal::class.java.name == type -> {
                        val value = BigDecimal(readString(JSON_SKIP))
                        jsonGenerator.writeNumber(value)
                        refs.add(value)
                        value
                    }
                    Timestamp::class.java.name == type
                            || Date::class.java.name == type
                            || Time::class.java.name == type -> {
                        val value = readDate(JSON_SKIP)
                        jsonGenerator.writeNumber(value)
                        refs.add(value)
                        value
                    }
                    else -> {
                        throw Hessian2UnexpectedException(currentJsonString(), "BigDecimal, sql time", type)
                    }
                }
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "object", codeName(tag))
            }
        }
    }

    /**
     * # value reference (e.g. circular trees and graphs)
     * ref        ::= x51 int            # reference to nth map/list/object
     */
    fun readRef(flag: Int = JSON_SKIP): Any {
        val tag = readTag()
        val value: Any
        return if (tag == 0x51) {
            val index = readInt(JSON_SKIP)
            value = refs[index]
            if (flag == JSON_FIELD_VALUE) {
                this.writeValueJson(value)
            }
            refs[index]
        } else {
            throw Hessian2UnexpectedException(currentJsonString(), "ref", codeName(tag))
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun writeValueJson(value: Any) {
        when (value) {
            is List<*> -> {
                this.writeArrayJson(value as List<Any>)
            }
            is Map<*, *> -> {
                this.writeMapJson(value as Map<String, Any>)
            }
            is String -> {
                jsonGenerator.writeString(value)
            }
            is Number -> {
                jsonGenerator.writeNumber(value.toString())
            }
            else -> {
                jsonGenerator.writeString(value.toString())
            }
        }
    }

    private fun writeArrayJson(list: List<Any>) {
        jsonGenerator.writeStartArray()
        list.forEach {
            writeValueJson(it)
        }
        jsonGenerator.writeEndArray()
    }

    private fun writeMapJson(map: Map<String, Any>) {
        jsonGenerator.writeStartObject()
        map.forEach { (key, value) ->
            jsonGenerator.writeFieldName(key)
            writeValueJson(value)
        }
        jsonGenerator.writeEndObject()
    }

    /**
     * # main production
     * value ::= null
     * value ::= binary   // ignore
     * value ::= boolean
     * value ::= class-def value
     * value ::= date
     * value ::= double
     * value ::= int
     * value ::= list
     * value ::= long
     * value ::= map
     * value ::= object // ignore
     * value ::= ref
     * value ::= string
     */
    fun readValue(flag: Int = JSON_SKIP): Any? =
        when {
            isNull() -> {
                this.readNull(flag)
                null
            }
            isBoolean() -> this.readBoolean(flag)
            isInt() -> this.readInt(flag)
            isLong() -> this.readLong(flag)
            isDouble() -> this.readDouble(flag)
            isList() -> this.readList()
            isMap() -> readMap()
            isString() -> this.readString(flag)
            isDate() -> this.readDate(flag)
            isClassDefinition() -> {
                this.readClassDefinition()
                this.readObject()
            }
            isObject() -> this.readObject()
            isRef() -> this.readRef(flag)
            else -> throw Hessian2UnexpectedException(currentJsonString(), "value", codeName(forward()))
        }

    /**
     * map ::= 'M' type (value value)* 'Z'  # key, value map pairs
     * map ::= 'H' (value value)* 'Z'       # untyped key, value
     */
    fun readMap(): Map<Any, Any?> {
        val tag = readTag()
        val map: MutableMap<Any, Any?> = HashMap()
        refs.add(map)
        when (tag) {
            'M'.toInt() -> {
                jsonGenerator.writeStartObject()
                this.readType()
                while (!isEnd()) {
                    map[this.readValue(JSON_FIELD_NAME)!!] = this.readValue(JSON_FIELD_VALUE)
                }
                readEnd()
                jsonGenerator.writeEndObject()
            }
            'H'.toInt() -> {
                jsonGenerator.writeStartObject()
                while (!isEnd()) {
                    map[this.readValue(JSON_FIELD_NAME)!!] = this.readValue(JSON_FIELD_VALUE)
                }
                readEnd()
                jsonGenerator.writeEndObject()
            }
            else -> {
                throw Hessian2UnexpectedException(currentJsonString(), "map", codeName(tag))
            }
        }
        return map
    }

    /**
     * # 8-bit binary data split into 64k chunks
     * binary ::= x41 b1 b0 <binary-data> binary # non-final chunk
     * binary ::= 'B' b1 b0 <binary-data>        # final chunk
     * binary ::= [x20-x2f] <binary-data>        # binary data of length 0-15
     * binary ::= [x34-x37] <binary-data>        # binary data of length 0-1023
     */
    fun readBytes(): ByteArray? {
        val tag = readTag()
        throw Hessian2UnexpectedException(currentJsonString(), "bytes", codeName(tag))
    }

    data class ClassDefinition(
        val type: String,
        val fieldName: List<String>
    )

}