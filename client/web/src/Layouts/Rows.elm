module Layouts.Rows exposing (Model, Msg, init, update, view, wrap)

import Browser exposing (Document)
import Html exposing (Html, div, form, h1, text)
import Html.Attributes exposing (class, id, style)
import Task



{- The layout technique is based on https://hackernoon.com/render-props-for-elm-d5547efd66f5 -}
{- TODO: Consider changing the interface to a list of columns instead of a tuple. Then we can handle
   all columnar layouts with this one module.
-}


type alias Model =
    { split : ( Int, Int ) }


type Msg parent
    = Parent parent


init : ( Int, Int ) -> Model
init split =
    { split = split }



-- UPDATE


update : Msg parent -> Model -> ( Model, Cmd parent )
update msg model =
    case msg of
        Parent p ->
            ( model, Task.perform identity (Task.succeed p) )



-- VIEW


wrap : List (Html parent) -> List (Html (Msg parent))
wrap =
    List.map (Html.map Parent)


percent : ( Int, Int ) -> ( String, String )
percent ( left, right ) =
    let
        part x =
            toFloat x / toFloat (left + right) * 100

        percentize num =
            String.fromFloat num ++ "%"
    in
    Tuple.mapBoth percentize percentize ( part left, part right )


view : Model -> ( List (Html parent), List (Html parent) ) -> Html (Msg parent)
view model ( left, right ) =
    let
        columnSplit =
            percent model.split
    in
    div
        [ class "two-column-layout"
        , style "display" "flex"
        , style "flex-direction" "column"
        , style "align-items" "stretch"
        , style "height" "100%"
        ]
        [ div
            [ style "background-color" "#decaff"
            , style "position" "relative"
            , style "width" "100%"
            , style "overflow-y" "auto"
            , style "flex-grow" <| String.fromInt (Tuple.first model.split)
            , style "max-height" <| Tuple.first columnSplit
            ]
            (wrap left)
        , div
            [ style "width" "100%"
            , style "position" "relative"
            , style "overflow-y" "auto"
            , style "flex-grow" <| String.fromInt (Tuple.second model.split)
            , style "max-height" <| Tuple.second columnSplit
            ]
            (wrap right)
        ]
