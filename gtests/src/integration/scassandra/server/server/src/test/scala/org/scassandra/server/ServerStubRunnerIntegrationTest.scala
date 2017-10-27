/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server

import java.io._
import java.net.Socket
import java.util

import akka.util.ByteString
import org.scassandra.codec._

import scala.collection.immutable.IndexedSeq

class ServerStubRunnerIntegrationTest extends AbstractIntegrationTest {
  var connectionToServerStub: Socket = _

  before {
    connectionToServerStub = ConnectionToServerStub()
  }

  after {
    println("Closing socket")
    connectionToServerStub.close()
  }

  test("return nothing until a startup message is received") {
    val bytes = availableBytes(200)

    bytes should equal(0)

    sendStartupMessage()

    val bytesAfterStartupMessage = availableBytes(200)
    bytesAfterStartupMessage should equal(8)
  }

  test("return a version byte in the response header") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // read first byte
    val responseHeaderVersion: Int = in.read()

    val responseHeaderVersionTwo = 0x82
    responseHeaderVersion should equal(responseHeaderVersionTwo)
  }

  test("return a flags byte in the response header with all bits set to 0 on STARTUP request") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first byte
    consumeBytes(in, 1)

    // read second byte
    val responseHeaderFlags: Int = in.read()

    val noCompressionFlags = 0x00
    responseHeaderFlags should equal(noCompressionFlags)
  }

  test("return a stream byte in the response header") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    val streamId : Byte = 0x05
    sendStartupMessage(streamId)

    // consume first two bytes
    consumeBytes(in, 2)

    // read third byte
    val responseHeaderStream: Int = in.read()

    // TODO: in future versions, the stream ID should reflect the value set in the request header
    responseHeaderStream should equal(streamId)
  }

  test("return a READY opCode byte in the response header on STARTUP request") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    sendStartupMessage()

    val responseHeaderOpCode: Int = readResponseHeaderOpCode

    responseHeaderOpCode should equal(Ready.opcode)
  }

  test("return length field with all 4 bytes set to 0 on STARTUP request") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first four bytes
    consumeBytes(in, 4)

    // read next four bytes
    var length = 0
    for (a <- 1 to 4) {
      length += in.read()
    }

    length should equal(0)
  }

  test("return RESULT OpCode on Query") {
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream
    implicit val inputStream : DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)

    sendStartupMessage()
    readReadyMessage()

    val query = "any old select"
    sendQueryMessage(query)

    val responseHeaderOpCode: Int = readResponseHeaderOpCode

    responseHeaderOpCode should equal(Result.opcode)
  }

  test("should reject query message if startup message has not been sent") {
    implicit val inputStream = new DataInputStream(connectionToServerStub.getInputStream)
    sendQueryMessage()

    // read fourth byte
    val responseHeaderOpCode: Int = readResponseHeaderOpCode()

    responseHeaderOpCode should equal(ErrorMessage.opcode)
  }

  test("test receiving size separately from opcode") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, Startup.opcode.toByte))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x04)) // 4 byte body for empty map.
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x00)) // empty STARTUP map.
    stream.flush()

    readReadyMessage()
  }

  test("test receiving body separately from opcode and size") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, Startup.opcode.toByte, 0x00, 0x00, 0x00, 0x16))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x0,0x1,0x0,0xb,0x43,0x51,0x4c,0x5f,0x56,0x45,0x52,0x53,0x49,0x4f,0x4e,0x00,0x05,0x33,0x2e,30,0x2e,30))
    stream.flush()

    readReadyMessage()
  }

  test("test receiving start of header separately to opcode") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x00, Startup.opcode.toByte, 0x00, 0x00, 0x00, 0x04)) // 4 byte body for empty map.
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x00)) // empty STARTUP map.
    stream.flush()

    readReadyMessage()
  }

  test("test receiving message in three parts") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, Startup.opcode.toByte, 0x00, 0x00, 0x00, 0x16))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x0,0x1,0x0,0xb,0x43,0x51))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x4c,0x5f,0x56,0x45,0x52,0x53,0x49,0x4f,0x4e,0x00,0x05,0x33,0x2e,30,0x2e,30))

    readReadyMessage()
  }

  test("test receiving whole messages after in multiple parts") {
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream
    implicit val inputStream : DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)

    stream.write(Array[Byte](0x02, 0x00, 0x00, Startup.opcode.toByte, 0x00, 0x00, 0x00, 0x16))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x0,0x1,0x0,0xb,0x43,0x51))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x4c,0x5f,0x56,0x45,0x52,0x53,0x49,0x4f,0x4e,0x00,0x05,0x33,0x2e,30,0x2e,30))

    readReadyMessage()
    sendQueryMessage("select * from people")
    val responseHeaderOpCode: Int = readResponseHeaderOpCode
    responseHeaderOpCode should equal(Result.opcode)
  }

  test("should return a result message with keyspace name on use statement") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream

    sendStartupMessage()
    readReadyMessage()

    sendQueryMessage("use people")

    val responseOpCode = readResponseHeaderOpCode()
    responseOpCode should equal(Result.opcode)

    val message = readMessageBody()
    println(s"Message body received ${util.Arrays.toString(message)}")

    val responseType = takeInt(message)
    responseType shouldEqual 0x3

    val cqlString = takeString(message.drop(4))
    cqlString.trim should equal("people")
  }

  test("Should return ready message for any register message") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream

    sendStartupMessage()
    readReadyMessage()

    // send a register message
    val registerMessage = Register().toBytes(0, Request)(ProtocolVersionV2).get
    stream.write(registerMessage.toArray)

    // read the op code
    val opCode = readResponseHeaderOpCode()
    opCode should equal(Ready.opcode)
  }

  def takeInt(bytes : Array[Byte]) = {
    ByteString(bytes.take(4)).asByteBuffer.getInt
  }

  def takeString(bytes : Array[Byte]) = {
    val stringLength = ByteString(bytes.take(2)).asByteBuffer.getShort
    new String(bytes.drop(2).take(stringLength))
  }

  def readMessageBody()(implicit inputStream : DataInputStream) = {
    // read the length
    val length = readInteger()
    println(s"Read length ${length}")
    // read the rest
    val messageBody = new Array[Byte](length)
    inputStream.read(messageBody)
    messageBody
  }

  def readReadyMessage() = {
    val stream: DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)
    consumeBytes(stream, 8)
  }

  def readResponseHeaderOpCode()(implicit inputStream : DataInputStream) = {
    // consume first three bytes
    consumeBytes(inputStream, 3)

    // read fourth byte
    inputStream.read()
  }

  def availableBytes(timeToWaitMillis: Long): Int = {
    // TODO: Make this check every N millis rather than wait the full amount first?
    Thread.sleep(timeToWaitMillis)
    val stream = new DataInputStream(connectionToServerStub.getInputStream)
    stream.available()
  }

  //TODO: Make this timeout
  def consumeBytes(stream: DataInputStream, numberOfBytes: Int) {
    for (i <- 1 to numberOfBytes) {
      stream.read()
    }
  }

  def sendStartupMessage(streamId : Byte = 0x00) = {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, streamId, Startup.opcode.toByte))
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x16))
    val fakeBody: IndexedSeq[Byte] = for (i <- 0 until 22) yield 0x00.toByte
    stream.write(fakeBody.toArray)
    stream.flush()
  }

  def sendQueryMessage(queryString : String = "select * from people") = {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    val queryMessage = Query(queryString).toBytes(0, Request)(ProtocolVersionV2).get
    stream.write(queryMessage.toArray)
  }

  def sendOptionsMessage() {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, Options.opcode.toByte))
    sendFakeLengthAndBody(stream)
  }

  def sendFakeLengthAndBody(stream: OutputStream) {
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x16))
    val fakeBody: IndexedSeq[Byte] = for (i <- 0 until 22) yield 0x00.toByte
    stream.write(fakeBody.toArray)
  }

  def readInteger()(implicit inputStream : DataInputStream) = {
    val bytes = new Array[Byte](4)
    inputStream.read(bytes)
    val byteString = ByteString(bytes)
    byteString.toByteBuffer.getInt
  }
}