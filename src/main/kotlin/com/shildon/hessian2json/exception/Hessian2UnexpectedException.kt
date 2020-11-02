package com.shildon.hessian2json.exception

/**
 * hessian2json解析异常
 *
 * @author shildon
 * @since 1.0.0
 */
class Hessian2UnexpectedException(
    json: String,
    expected: String,
    actual: String
) : RuntimeException("parse hessian2 error, json: [$json], expected: [$expected], actual: [$actual]")
