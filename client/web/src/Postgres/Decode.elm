module Postgres.Decode exposing (Resultset, decodeQueryResponse, resultsetDecoder)

import Bytes exposing (Bytes, Endianness(..))
import Bytes.Decode as Bytes exposing (Decoder, Step(..), andThen, bytes, decode, loop, map, signedInt16, signedInt32, string, succeed, unsignedInt16, unsignedInt32, unsignedInt8)



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

   ## Request

   510000002473656c656374202a2066726f6d2070675f737461745f61637469766974793b00

   Q$select * from pg_stat_activity;


-}


decodeQueryResponse : Maybe Bytes -> Maybe Resultset
decodeQueryResponse r =
    case r of
        Just b ->
            Bytes.decode resultsetDecoder b

        Nothing ->
            Nothing


type alias Resultset =
    { tag : String
    , len : Int
    , fields : List ColumnInfo
    , rows : DataRow
    }


type alias DataRow =
    { tag : String
    , len : Int
    , values : List String
    }


resultsetDecoder : Decoder Resultset
resultsetDecoder =
    succeed Resultset
        |> read (string 1)
        |> read (unsignedInt32 BE)
        |> read
            (unsignedInt16 BE
                |> andThen
                    (\n ->
                        list n columnInfo
                    )
            )
        |> read dataRow


dataRow : Decoder DataRow
dataRow =
    succeed DataRow
        |> read (string 1)
        |> read (unsignedInt32 BE)
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
   Int16       The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.

-}


type alias ColumnInfo =
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


columnInfo : Decoder ColumnInfo
columnInfo =
    succeed ColumnInfo
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


read : Decoder a -> Decoder (a -> b) -> Decoder b
read decoder prev =
    andThen (\p -> map p decoder) prev


skip : Decoder a -> Decoder b -> Decoder b
skip decoder prev =
    andThen (\p -> map (always p) decoder) prev


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


cstring : Decoder String
cstring =
    let
        step str =
            string 1
                |> Bytes.andThen
                    (\char ->
                        succeed <|
                            if char /= "\u{0000}" then
                                Loop (char :: str)

                            else
                                Done (List.reverse str |> String.join "")
                    )
    in
    loop [] step
