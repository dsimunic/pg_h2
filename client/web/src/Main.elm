module Main exposing (Model, Msg(..), init, main, update, view)

import Browser
import Bytes exposing (Bytes)
import Hex
import Html exposing (Html, div, h1, img, table, td, text, th, tr)
import Html.Attributes exposing (style)
import Http exposing (header)
import Layouts.Columns as Layout
import Postgres.Decode exposing (Resultset, decodeQueryResponse, resultsetDecoder)


type Page
    = InputPage
    | DisplayPage


type Msg
    = NoOp
    | ChangeQueryText String
    | SubmitQuery
    | GotQueryResult (Result Http.Error Resultset)
    | GotLayoutMsg (Layout.Msg Msg)


type alias Model =
    { currPage : Page
    , layoutModel : Layout.Model
    , currPlanText : String
    , bytes : Maybe Resultset
    , raw : Maybe Bytes
    }


type alias Flags =
    ()


init : Flags -> ( Model, Cmd Msg )
init _ =
    ( { currPage = DisplayPage
      , currPlanText = "select * from pg_stat_activity;"
      , bytes = Nothing
      , raw =
            Hex.toBytes <|
                "540000022a001364617469640000002e1500010000001a0004ffffffff00006461746e616d650000002e150002000000130040ffffffff00007069640000002e150003000000170004ffffffff000075736573797369640000002e1500040000001a0004ffffffff00007573656e616d650000002e150005000000130040ffffffff00006170706c69636174696f6e5f6e616d650000002e15000600000019ffffffffffff0000636c69656e745f616464720000002e15000700000365ffffffffffff0000636c69656e745f686f73746e616d650000002e15000800000019ffffffffffff0000636c69656e745f706f72740000002e150009000000170004ffffffff00006261636b656e645f73746172740000002e15000a000004a00008ffffffff0000786163745f73746172740000002e15000b000004a00008ffffffff000071756572795f73746172740000002e15000c000004a00008ffffffff000073746174655f6368616e67650000002e15000d000004a00008ffffffff0000776169745f6576656e745f747970650000002e15000e00000019ffffffffffff0000776169745f6576656e740000002e15000f00000019ffffffffffff000073746174650000002e15001000000019ffffffffffff00006261636b656e645f7869640000002e1500110000001c0004ffffffff00006261636b656e645f786d696e0000002e1500120000001c0004ffffffff000071756572790000002e15001300000019ffffffffffff0000"
                    ++ "4400000129001300000007373631393539350000001863635f6b6e6f776c656467652d676174657761792e6f726700000005323532383800000005313734393300000003656373000000047073716c000000033a3a31ffffffff0000000536333030370000001d323031392d30332d30332031363a32333a33312e3338313739382b30310000001d323031392d30332d30332031363a32333a33382e3331363137352b30310000001d323031392d30332d30332031363a32333a33382e3331363137352b30310000001d323031392d30332d30332031363a32333a33382e3331383030372b3031ffffffffffffffff00000006616374697665ffffffff000000063334343236360000001f73656c656374202a2066726f6d2070675f737461745f61637469766974793b"
      , layoutModel = Layout.init ( 1, 2 )
      }
    , Cmd.none
    )



-- SUBSCRIPTIONS


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none



-- UPDATE


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ChangeQueryText s ->
            ( { model | currPlanText = s }, Cmd.none )

        SubmitQuery ->
            ( model, issueQuery model.currPlanText )

        GotQueryResult (Ok r) ->
            ( { model | bytes = Just r }, Cmd.none )

        GotLayoutMsg subMsg ->
            let
                ( layoutModel, subCmd ) =
                    Layout.update subMsg model.layoutModel
            in
            ( { model | layoutModel = layoutModel }, subCmd )

        _ ->
            ( model, Cmd.none )


issueQuery : String -> Cmd Msg
issueQuery q =
    Http.request
        { method = "QUERY"
        , headers =
            [ header "database" "postgres"
            , header "dbhost" "paris-scw-01.influent.cc"
            , header "dbport" "65432"
            , header "user" "user2(md5)"
            , header "password" "YzQzOGI2MmNiN2Vm"
            ]
        , url = "https://localhost:8443"
        , body = Http.stringBody "postgres/simple-query" q
        , expect = Http.expectBytes GotQueryResult resultsetDecoder
        , timeout = Nothing
        , tracker = Nothing
        }



-- VIEW


view : Model -> Browser.Document Msg
view model =
    let
        content =
            case model.currPage of
                DisplayPage ->
                    displayPage model

                InputPage ->
                    inputPage model
    in
    { title = "pg_http"
    , body = [ div [ style "height" "100vh" ] [ content ] ]
    }


subView : Layout.Model -> ( List (Html Msg), List (Html Msg) ) -> Html Msg
subView subModel =
    Html.map GotLayoutMsg << Layout.view subModel


inputPage : Model -> Html Msg
inputPage model =
    div [] []


displayPage : Model -> Html Msg
displayPage ({ layoutModel } as model) =
    subView
        layoutModel
        ( [ div [] [ Html.text model.currPlanText ] ]
        , [ div [] [ rowsetInfo <| decodeQueryResponse model.raw ] ]
        )


rowsetInfo : Maybe Resultset -> Html Msg
rowsetInfo r =
    case r of
        Just { tag, len, fields, rows } ->
            table []
                [ tr [] (List.map (.name >> (\h -> th [] [ text h ])) fields)
                , tr [] (List.map (\v -> td [] [ text v ]) rows.values)
                ]

        Nothing ->
            div [] [ text "Failed to decode!" ]



-- PROGRAM


main : Program Flags Model Msg
main =
    Browser.document
        { init = init
        , update = update
        , view = view
        , subscriptions = subscriptions
        }
