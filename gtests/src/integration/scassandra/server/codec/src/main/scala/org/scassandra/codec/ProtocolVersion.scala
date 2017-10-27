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
package org.scassandra.codec

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.scassandra.codec.Notations.{short => cshort, string => cstring}
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages._
import scodec.Attempt.Successful
import scodec.Codec
import scodec.codecs._

/**
  * As the native protocol is versioned, we tie protocol-version specific behavior to this trait.
  *
  * As new protocol versions are added, one simply just needs to define new implementations of [[ProtocolVersion]] and
  * register it as part of [[ProtocolVersion.codec]].
  */
sealed trait ProtocolVersion {
  /**
    * The version of this protocol.
    */
  val version: Int
  /**
    * The length, in bytes, of the frame header for this protocol version.  This is generally between 8 and 9 bytes, to
    * accompany the stream id length change between prtocol 3 and 4 from 1 to 2 bytes.
    */
  lazy val headerLength: Long = 7 + (streamIdCodec.sizeBound.exact.get / 8)

  /**
    * Used to cache [[Message]] codecs as they are created.  This prevents repeated creation of codecs.
    */
  private[this] lazy val messageCodecCache = CacheBuilder.newBuilder
    .build(new CacheLoader[Integer, Codec[Message]]() {
      override def load(key: Integer): Codec[Message] = Message.codecForVersion(ProtocolVersion.this, key)
    })

  /**
    * Used to cache [[DataType]] codecs as they are created.
    */
  private[this] lazy val dataTypeCodecCache = CacheBuilder.newBuilder
    .build(new CacheLoader[DataType,DataTypeCodecs]() {
      override def load(key: DataType): DataTypeCodecs = DataTypeCodecs(key)
    })

  /**
    * Used to cache [[Row]] codecs as they are created.
    */
  private[this] lazy val rowCodecCache = CacheBuilder.newBuilder
    .maximumSize(1000)
    .build(new CacheLoader[List[ColumnSpec], Codec[Row]]() {
      override def load(key: List[ColumnSpec]): Codec[Row] = Row.withColumnSpec(key)(ProtocolVersion.this)
    })

  // codecs that have protocol version specific behavior.
  private[codec] val streamIdCodec: Codec[Int]
  private[codec] val collectionLengthCodec: Codec[Int]
  private[codec] val dataTypeCodec: Codec[DataType]
  private[codec] def messageCodec(opcode: Int): Codec[Message] = messageCodecCache.get(opcode)
  private[codec] lazy val batchCodec: Codec[Batch] = Batch.codecForVersion(this)
  private[codec] lazy val executeCodec: Codec[Execute] = Execute.codecForVersion(this)
  private[codec] lazy val preparedCodec: Codec[Prepared] = Prepared.codecForVersion(this)
  private[codec] lazy val preparedMetadataCodec: Codec[PreparedMetadata] = PreparedMetadata.codecForVersion(this)
  private[codec] lazy val queryCodec: Codec[Query] = Query.codecForVersion(this)
  private[codec] lazy val queryParametersCodec: Codec[QueryParameters] = QueryParameters.codecForVersion(this)
  private[codec] lazy val resultCodec: Codec[Result] = Result.codecForVersion(this)
  private[codec] lazy val rowMetadataCodec: Codec[RowMetadata] = RowMetadata.codecForVersion(this)
  private[codec] lazy val rowsCodec: Codec[Rows] = Rows.codecForVersion(this)
  private[codec] lazy val columnSpecWithTableCodec: Codec[ColumnSpec] = ColumnSpec.codecForVersion(withTable = true)(this)
  private[codec] lazy val columnSpecWithoutTableCodec: Codec[ColumnSpec] = ColumnSpec.codecForVersion(withTable = false)(this)
  private[codec] def dataTypeCodec(dataType: DataType): Codec[Any] = dataTypeCodecCache.get(dataType).codec
  private[codec] def rowCodec(columnSpec: List[ColumnSpec]): Codec[Row] = rowCodecCache.get(columnSpec)

  private[this] def createHeaderCodec(protocolFlags: ProtocolFlags) =
    provide(protocolFlags).flatPrepend(FrameHeader.withProtocol).as[FrameHeader]

  private[this] lazy val requestFlags = ProtocolFlags(Request, this)
  private[this] lazy val responseFlags = ProtocolFlags(Response, this)

  private[this] lazy val requestHeaderCodec: Codec[FrameHeader] = createHeaderCodec(requestFlags)
  private[this] lazy val responseHeaderCodec: Codec[FrameHeader] = createHeaderCodec(responseFlags)

  private[codec] def headerCodec(direction: MessageDirection): Codec[FrameHeader] = direction match {
    case Request => requestHeaderCodec
    case Response => responseHeaderCodec
  }

  /**
    * Container that generates and maintains [[Codec]]s for a given [[DataType]]
    * @param dataType The type to generate codecs for.
    */
  private[this] case class DataTypeCodecs(dataType: DataType) {
    import DataType._
    val baseCodec = dataType.baseCodec(ProtocolVersion.this)
    val codec = baseCodec.asInstanceOf[Codec[Any]].withAny(dataType.native)
  }
}


/**
  * Provides a stream id codec of 1 byte in size.  This should only be used for [[ProtocolVersionV1]] and
  * [[ProtocolVersionV2]].
  */
sealed trait Int8StreamId {
  val streamIdCodec = int8
}

/**
  * Provides a stream id codec of 2 bytes in size.  It is expected that all [[ProtocolVersion]] implementations from
  * [[ProtocolVersionV3]] on up use this.
  */
sealed trait Int16StreamId {
  val streamIdCodec = int16
}

/**
  * Provides a collection length codec of 16-bits in size.  This should only be used for [[ProtocolVersionV1]] and
  * [[ProtocolVersionV2]].
  */
sealed trait Uint16CollectionLength {
  val collectionLengthCodec = int16
}

/**
  * Provides a collection length codec of 2 bytes in size.  It is expected that all [[ProtocolVersion]] implementations
  * from [[ProtocolVersionV3]] on up use this.
  */
sealed trait Uint32CollectionLength {
  val collectionLengthCodec = int32
}

case object ProtocolVersionV1 extends ProtocolVersion
  with Int8StreamId
  with Uint16CollectionLength {
  override val version = 1
  override val dataTypeCodec = ProtocolVersion.v1v2DataTypeCodec
}

case object ProtocolVersionV2 extends ProtocolVersion
  with Int8StreamId
  with Uint16CollectionLength {
  override val version = 2
  override val dataTypeCodec = ProtocolVersion.v1v2DataTypeCodec
}

case object ProtocolVersionV3 extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  override val version = 3
  override val dataTypeCodec = ProtocolVersion.v3DataTypeCodec()
}

case object ProtocolVersionV4 extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  override val version = 4
  override val dataTypeCodec = ProtocolVersion.v4DataTypeCodec()
}

/**
  * Defines an arbitrary [[ProtocolVersion]] that is not currently supported by this library
  * @param version Arbitrary version that is not supported.
  */
case class UnsupportedProtocolVersion(version: Int) extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  // doesn't particularly matter for this case.
  override val dataTypeCodec = ProtocolVersion.v1v2DataTypeCodec
}

object ProtocolVersion {

  /**
    * Decodes 7 bits as an integer into a [[ProtocolVersion]] instance.  If there is no [[ProtocolVersion]] for the
    * given value, [[UnsupportedProtocolVersion]] is returned.
    */
  implicit val codec: Codec[ProtocolVersion] = uint(7).narrow({
    case 1 => Successful(ProtocolVersionV1)
    case 2 => Successful(ProtocolVersionV2)
    case 3 => Successful(ProtocolVersionV3)
    case 4 => Successful(ProtocolVersionV4)
    case x => Successful(UnsupportedProtocolVersion(x))
  }, _.version)

  /**
    * All currently supported [[ProtocolVersion]]s.
    */
  val versions: List[ProtocolVersion] = ProtocolVersionV1 :: ProtocolVersionV2 :: ProtocolVersionV3 ::
    ProtocolVersionV4 :: Nil

  /**
    * The newest [[ProtocolVersion]] supported.
    */
  val latest: ProtocolVersion = ProtocolVersionV4

  // base descriminator for DataType.
  private[this] def baseDesc = discriminated[DataType].by(cshort)

  // The v1/v2 codec minus the text type.
  // NOTE: That the odd behavior of passing in base discriminators is to facilitate lazily evaluated codecs
  // (tuples, lists, etc.) that need a forward reference to the codec to encode/decode elements.
  private[this] def baseDataTypeCodec(base: DiscriminatorCodec[DataType, Int]) = {
    lazy val codec: DiscriminatorCodec[DataType, Int] = base
      .typecase(0x00, cstring.as[DataType.Custom])
      .typecase(0x01, provide(DataType.Ascii))
      .typecase(0x02, provide(DataType.Bigint))
      .typecase(0x03, provide(DataType.Blob))
      .typecase(0x04, provide(DataType.Boolean))
      .typecase(0x05, provide(DataType.Counter))
      .typecase(0x06, provide(DataType.Decimal))
      .typecase(0x07, provide(DataType.Double))
      .typecase(0x08, provide(DataType.Float))
      .typecase(0x09, provide(DataType.Int))
      .typecase(0x0B, provide(DataType.Timestamp))
      .typecase(0x0C, provide(DataType.Uuid))
      .typecase(0x0D, provide(DataType.Varchar))
      .typecase(0x0E, provide(DataType.Varint))
      .typecase(0x0F, provide(DataType.Timeuuid))
      .typecase(0x10, provide(DataType.Inet))
      .typecase(0x20, lazily(codec.as[DataType.List]))
      .typecase(0x21, lazily((codec :: codec).as[DataType.Map]))
      .typecase(0x22, lazily(codec.as[DataType.Set]))
    codec
  }

  // v1/v2 includes a Text type at opcode 0xA/10.  This was removed in protocol v3.
  private[codec] def v1v2DataTypeCodec: DiscriminatorCodec[DataType, Int] =
    baseDataTypeCodec(baseDesc.typecase(0x0A, provide(DataType.Text)))

  // v3 introduces tuples and removes the text type.
  private[codec] def v3DataTypeCodec(base: Option[DiscriminatorCodec[DataType, Int]] = None) = {
    lazy val codec: DiscriminatorCodec[DataType, Int] = baseDataTypeCodec(
      base.getOrElse(baseDesc)
        .typecase(0x0D, provide(DataType.Text)) // As Text type has been removed, alias it to varchars opcode.
        .typecase(0x31, lazily(cshort.consume(count => listOfN(provide(count), codec))(_.size).as[DataType.Tuple]))
    )
    codec
  }

  // v4 introduces date, time, smallint and tinyint types.
  private[codec] def v4DataTypeCodec(base: Option[DiscriminatorCodec[DataType, Int]] = None) = v3DataTypeCodec(
    Some(base.getOrElse(baseDesc)
      .typecase(0x11, provide(DataType.Date))
      .typecase(0x12, provide(DataType.Time))
      .typecase(0x13, provide(DataType.Smallint))
      .typecase(0x14, provide(DataType.Tinyint)))
  )

}
