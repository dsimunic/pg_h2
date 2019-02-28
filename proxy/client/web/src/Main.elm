module Main exposing (Model, Msg(..), init, main, update, view)

import Browser
import Bytes exposing (Bytes)
import Bytes.Decode as Bytes
import Element exposing (..)
import Element.Background as Background
import Element.Border as Border
import Element.Font as Font
import Element.Input as Input
import Html exposing (Html, div, h1, img)
import Http exposing (header)


type Page
    = InputPage
    | DisplayPage


type Resultset
    = Resultset Bytes


type Msg
    = NoOp
    | ChangeQueryText String
    | SubmitQuery
    | GotQueryResult (Result Http.Error Resultset)


type alias Model =
    { currPage : Page
    , currPlanText : String
    }


type alias Flags =
    ()


init : Flags -> ( Model, Cmd Msg )
init _ =
    ( { currPage = InputPage
      , currPlanText = "select * from pg_stat_activity;"
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
        , expect = Http.expectBytes GotQueryResult rowsetDecoder
        , timeout = Nothing
        , tracker = Nothing
        }


rowsetDecoder : Bytes.Decoder Resultset
rowsetDecoder =
    Bytes.map Resultset <| Bytes.bytes 2



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
    { title = "VisExp"
    , body =
        [ layout [] <|
            column [ width fill, spacingXY 0 20 ]
                [ navBar
                , content
                ]
        ]
    }


blue : Color
blue =
    rgb255 52 101 164


lightCharcoal : Color
lightCharcoal =
    rgb255 222 202 255


green : Color
green =
    rgb255 222 250 206


darkGreen : Color
darkGreen =
    rgb255 186 202 186


white : Color
white =
    rgb 1 1 1


navBar : Element Msg
navBar =
    row
        [ width fill
        , paddingXY 60 10
        , Border.widthEach { bottom = 1, top = 0, left = 0, right = 0 }
        , Border.color blue
        ]
        [ el [ alignLeft ] <| text "Pg http2"
        ]


inputPage : Model -> Element Msg
inputPage model =
    column
        [ width (px 800)
        , spacingXY 0 10
        , centerX
        ]
        [ Input.multiline
            [ height (px 300)
            , Border.width 1
            , Border.rounded 3
            , Border.color lightCharcoal
            , padding 3
            ]
            { onChange = ChangeQueryText
            , text = model.currPlanText
            , placeholder = Nothing
            , label =
                Input.labelAbove [] <|
                    text "Query"
            , spellcheck = False
            }
        , Input.button
            [ Background.color darkGreen
            , Border.color darkGreen
            , Border.rounded 3
            , Border.widthEach { bottom = 3, top = 0, right = 0, left = 0 }
            , Font.bold
            , Font.color white
            , paddingXY 20 6
            , alignRight
            , width (px 200)
            , height (px 40)
            ]
            { onPress = Just SubmitQuery
            , label = el [ centerX ] <| text "Go!"
            }
        ]


displayPage : Model -> Element Msg
displayPage model =
    column [] [ text model.currPlanText ]



-- PROGRAM


main : Program Flags Model Msg
main =
    Browser.document
        { init = init
        , update = update
        , view = view
        , subscriptions = subscriptions
        }
