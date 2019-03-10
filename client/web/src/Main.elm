module Main exposing (Model, Msg(..), init, main, update, view)

-- import Layouts.Columns as Layout

import Browser
import Bytes exposing (Bytes)
import Hex
import Html exposing (Html, button, div, h1, img, input, table, td, text, textarea, th, tr)
import Html.Attributes exposing (style, type_, value)
import Html.Events exposing (onClick, onInput)
import Http exposing (header)
import Layouts.Rows as Layout
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
    , queryText : String
    , bytes : Maybe Resultset
    }


type alias Flags =
    ()


init : Flags -> ( Model, Cmd Msg )
init _ =
    ( { currPage = InputPage
      , queryText = "select * from pg_settings;"
      , bytes = Nothing
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
            ( { model | queryText = s }, Cmd.none )

        SubmitQuery ->
            ( model, issueQuery model.queryText )

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
        , headers = []
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
inputPage ({ layoutModel } as model) =
    subView
        layoutModel
        ( [ div [ style "padding" "0.7em" ]
                [ textarea [ value model.queryText, onInput ChangeQueryText, style "width" "100%" ] []
                , button [ onClick SubmitQuery ] [ text "run query" ]
                ]
          ]
        , [ div [ style "background-color" "#eaeaea", style "padding" "0.7em" ]
                [ case model.bytes of
                    Just _ ->
                        rowsetInfo model.bytes

                    Nothing ->
                        text ""
                ]
          ]
        )


displayPage : Model -> Html Msg
displayPage ({ layoutModel } as model) =
    subView
        layoutModel
        ( [ div [] [ Html.text model.queryText ] ]
        , [ div [] [ rowsetInfo model.bytes ] ]
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
