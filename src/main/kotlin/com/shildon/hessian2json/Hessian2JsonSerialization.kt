package com.shildon.hessian2json

import com.fasterxml.jackson.core.JsonFactory
import org.apache.dubbo.common.URL
import org.apache.dubbo.common.serialize.ObjectInput
import org.apache.dubbo.common.serialize.ObjectOutput
import org.apache.dubbo.common.serialize.Serialization
import java.io.InputStream
import java.io.OutputStream

/**
 * @author shildon
 * @since 1.0.0
 */
class Hessian2JsonSerialization : Serialization {

    companion object {
        const val ID = 2
    }

    private val jsonFactory = JsonFactory()

    override fun getContentTypeId(): Byte = ID.toByte()

    override fun getContentType(): String = "x-application/hessian2"

    override fun serialize(url: URL, output: OutputStream): ObjectOutput {
        return Hessian2ObjectOutput(output)
    }

    override fun deserialize(url: URL, input: InputStream): ObjectInput {
        val genericKey = "generic"
        val trueValue = "true"
        val sideKey = "side"
        val consumerValue = "consumer"
        // only use hessian2json when generic == true, consumer side and handle response with value
        if (
            trueValue == url.getParameter(genericKey)
            && consumerValue == url.getParameter(sideKey)
            && input.markSupported()
        ) {
            input.mark(1)
            val flag = input.read() - 0x90
            input.reset()
            val value = 1
            val valueWithAttachments = 4
            if (flag == value || flag == valueWithAttachments) {
                return Hessian2JsonObjectInput(input, jsonFactory)
            }
        }
        return Hessian2ObjectInput(input)
    }

}