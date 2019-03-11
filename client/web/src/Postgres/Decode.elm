module Postgres.Decode exposing (PgMsg, Resultset, decodeResultset, decodeResponseRaw, expectPostgresResponse, expectRawPostgresResponse)

import Bytes exposing (Bytes, Endianness(..))
import Bytes.Decode as Bytes exposing (Decoder, Step(..), andThen, bytes, decode, loop, map, map2, signedInt16, signedInt32, string, succeed, unsignedInt16, unsignedInt32, unsignedInt8)
import Http



{-

   ## RowDescription

   Byte1('T')  Identifies the message as a row description.
   Int32       Length of message contents in bytes, including self.
   Int16       Specifies the number of fields in a row (can be zero).

   ### FieldDescription

   String      The field name.
   Int32       If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
   Int16       If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
   Int32       The object ID of the field's data type.
   Int16       The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
   Int32       The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
   Int16       The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.


   ## DataRow

   Byte1('D')  Identifies the message as a data row.
   Int32       Length of message contents in bytes, including self.
   Int16       The number of column values that follow (possibly zero).

   ### ColumnValue

   Int32       The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
   Byte len    The value of the column, in the format indicated by the associated format code. len is the above length.

-}


type alias PgMsg =
    { tag : String, length : Int, payload : Bytes }


decodeResponseRaw : Bytes -> Maybe (List PgMsg)
decodeResponseRaw b =
    Bytes.decode (rawMessages <| Bytes.width b) b



{-
   This is how the raw decoded rows look like:

   [{ length = 462, payload = <462 bytes>, tag = "T" }
   ,{ length = 228, payload = <228 bytes>, tag = "D" }
   ,{ length = 196, payload = <196 bytes>, tag = "D" }
   ]

   To get those to a resultset, we need to

   a) Extract the head
   b) Map a list of PgMsg to a list of DataRow
-}


decodeResultset : Bytes -> Maybe Resultset
decodeResultset b =
    Maybe.map rawToResultset <| decodeResponseRaw b


rawToResultset : List PgMsg -> Resultset
rawToResultset raw =
    List.partition (\m -> m.tag == "T") raw
        |> Tuple.mapBoth (List.head >> Maybe.andThen rawToFieldDescription >> Maybe.withDefault []) (List.filterMap rawToRow)
        |> (\( f, r ) -> Resultset f r)



-- HTTP Helpers


expectRawPostgresResponse : (Result Http.Error (List PgMsg) -> msg) -> Http.Expect msg
expectRawPostgresResponse toMsg =
    expectPostgresResponse toMsg decodeResponseRaw


expectPostgresResponse : (Result Http.Error a -> msg) -> (Bytes -> Maybe a) -> Http.Expect msg
expectPostgresResponse toMsg toPgmsg =
    Http.expectBytesResponse toMsg <|
        \response ->
            case response of
                Http.BadUrl_ url ->
                    Err (Http.BadUrl url)

                Http.Timeout_ ->
                    Err Http.Timeout

                Http.NetworkError_ ->
                    Err Http.NetworkError

                Http.BadStatus_ metadata body ->
                    Err (Http.BadStatus metadata.statusCode)

                Http.GoodStatus_ metadata body ->
                    case toPgmsg body of
                        Just value ->
                            Ok value

                        Nothing ->
                            Err <| Http.BadBody "Could not decode posgtres query response"


rawToFieldDescription : PgMsg -> Maybe (List FieldDescription)
rawToFieldDescription { payload } =
    Bytes.decode fieldDescriptionList payload


rawToRow { payload } =
    Bytes.decode dataRow payload


rawMessages : Int -> Decoder (List PgMsg)
rawMessages w =
    let
        step ( msgs, remain ) =
            if remain <= 0 then
                succeed (Done <| List.reverse msgs)

            else
                map (\x -> Loop ( x :: msgs, remain - .length x - 5 )) rawMessage
    in
    loop ( [], w ) step


rawMessage : Decoder PgMsg
rawMessage =
    string 1
        |> andThen (succeed << PgMsg)
        |> andThen
            (\p ->
                unsignedInt32 BE
                    |> andThen (\i -> map (p <| i - 4) (bytes <| i - 4))
            )


type alias Resultset =
    { fields : List FieldDescription
    , rows : List DataRow
    }


type alias DataRow =
    { values : List String
    }


dataRow : Decoder DataRow
dataRow =
    succeed DataRow
        |> read (unsignedInt16 BE |> andThen (\n -> list n columnValue))


columnValue : Decoder String
columnValue =
    signedInt32 BE
        |> andThen
            (\len ->
                if len > 0 then
                    string len

                else
                    succeed ""
            )



{-
   ### FieldDescription

   String      The field name.
   Int32       If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
   Int16       If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
   Int32       The object ID of the field's data type.
   Int16       The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
   Int32       The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
   Int16       The format code being used for the field. Currently will be zero (text) or one (binary).
               In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.

-}


type alias FieldDescription =
    { name : String
    , tableOID : Int
    , columnIndex : Int
    , typeOID : Int
    , columnLength : Int
    , typeModifier : Int
    , format : ColumnFormat
    }


type ColumnFormat
    = Text
    | Binary


fieldDescriptionList : Decoder (List FieldDescription)
fieldDescriptionList =
    unsignedInt16 BE |> andThen (\n -> list n fieldDescription)


fieldDescription : Decoder FieldDescription
fieldDescription =
    succeed FieldDescription
        |> read cstring
        |> read (unsignedInt32 BE)
        |> read (unsignedInt16 BE)
        |> read (unsignedInt32 BE)
        |> read (signedInt16 BE)
        |> read (Bytes.signedInt32 BE)
        |> read columnFormat


columnFormat : Decoder ColumnFormat
columnFormat =
    map
        (\f ->
            if f == 0 then
                Text

            else
                Binary
        )
        (unsignedInt16 BE)


cstring : Decoder String
cstring =
    let
        step str =
            string 1
                |> andThen
                    (\char ->
                        succeed <|
                            if char /= "\u{0000}" then
                                Loop (char :: str)

                            else
                                Done (List.reverse str |> String.join "")
                    )
    in
    loop [] step


read : Decoder a -> Decoder (a -> b) -> Decoder b
read decoder prev =
    andThen (\p -> map p decoder) prev


skip : Decoder ignore -> Decoder keep -> Decoder keep
skip decoderIgnore decoderKeep =
    andThen (\p -> map (always p) decoderIgnore) decoderKeep


list : Int -> Decoder a -> Decoder (List a)
list len decoder =
    let
        listStep dec ( n, xs ) =
            if n <= 0 then
                succeed (Done <| List.reverse xs)

            else
                map (\x -> Loop ( n - 1, x :: xs )) dec
    in
    loop ( len, [] ) (listStep decoder)
