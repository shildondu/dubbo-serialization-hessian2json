package com.shildon.hessian2json

import com.alibaba.com.caucho.hessian.io.Hessian2Output
import org.apache.dubbo.common.serialize.ObjectOutput
import java.io.OutputStream

/**
 * @author shildon
 * @since 1.0.0
 */
class Hessian2ObjectOutput(
    outputStream: OutputStream
) : ObjectOutput {

    private val hessian2Output = Hessian2Output(outputStream)

    override fun writeBool(v: Boolean) {
        hessian2Output.writeBoolean(v)
    }

    override fun writeByte(v: Byte) {
        hessian2Output.writeInt(v.toInt())
    }

    override fun writeShort(v: Short) {
        hessian2Output.writeInt(v.toInt())
    }

    override fun writeInt(v: Int) {
        hessian2Output.writeInt(v)
    }

    override fun writeLong(v: Long) {
        hessian2Output.writeLong(v)
    }

    override fun writeFloat(v: Float) {
        hessian2Output.writeDouble(v.toDouble())
    }

    override fun writeDouble(v: Double) {
        hessian2Output.writeDouble(v)
    }

    override fun writeUTF(v: String?) {
        hessian2Output.writeString(v)
    }

    override fun writeBytes(v: ByteArray?) {
        hessian2Output.writeBytes(v)
    }

    override fun writeBytes(v: ByteArray?, off: Int, len: Int) {
        hessian2Output.writeBytes(v, off, len)
    }

    override fun flushBuffer() {
        hessian2Output.flushBuffer()
    }

    override fun writeObject(obj: Any?) {
        hessian2Output.writeObject(obj)
    }

}