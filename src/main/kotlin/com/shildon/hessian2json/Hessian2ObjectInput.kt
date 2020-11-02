package com.shildon.hessian2json

import com.alibaba.com.caucho.hessian.io.Hessian2Input
import org.apache.dubbo.common.serialize.ObjectInput
import java.io.InputStream
import java.lang.reflect.Type

/**
 * @author shildon
 * @since 1.0.0
 */
class Hessian2ObjectInput(
    inputStream: InputStream
) : ObjectInput {

    private val hessian2Input = Hessian2Input(inputStream)

    override fun readBool(): Boolean {
        return hessian2Input.readBoolean()
    }

    override fun readByte(): Byte {
        return hessian2Input.readInt().toByte()
    }

    override fun readShort(): Short {
        return hessian2Input.readInt().toShort()
    }

    override fun readInt(): Int {
        return hessian2Input.readInt()
    }

    override fun readLong(): Long {
        return hessian2Input.readLong()
    }

    override fun readFloat(): Float {
        return hessian2Input.readDouble().toFloat()
    }

    override fun readDouble(): Double {
        return hessian2Input.readDouble()
    }

    override fun readUTF(): String? {
        return hessian2Input.readString()
    }

    override fun readBytes(): ByteArray? {
        return hessian2Input.readBytes()
    }

    override fun readObject(): Any? {
        return hessian2Input.readObject()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> readObject(cls: Class<T>?): T? {
        return hessian2Input.readObject(cls) as T?
    }

    override fun <T : Any?> readObject(cls: Class<T>?, type: Type?): T? {
        return this.readObject(cls)
    }

}