#light
(*
   Copyright 2009 Mindset Media

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*)

// This sample computes the n-queens problem using streams
// Briefly, that is for a n-by-n chessboard, arrange n queens
// such that no two queens lie in the same row, column or diagonal.
// (this is equivalent to saying that none of the queens can take 
// eachother in chess). For more information, see:
// http://en.wikipedia.org/wiki/Eight_queens_puzzle
// Idea for this solution comes from Streams lecture from SICP 
// (available from MIT opencourseware)
//
// One interesting thing about using Aqueduct to solve this problem is that
// it parallelizes the search -- each row is run in its own thread.
//
// The output is the list of solutions in tabular format:
// for instance, for 4 queens, the results are:
// Row 1   Row 2   Row 3   Row 4
// 2       4       1       3
// 3       1       4       2
// This means there are 2 solutions found (one per row of output). 
// The first solution has queens in (1,2) (2,4) (3,1) and (4,3)
// The second solution has queens in (1,3) (2,1) (3,4) and (4,2)

module NQueens

open Aqueduct
open Aqueduct.Common
open Aqueduct.Primitives
open Aqueduct.Pipes

// Create some schemas. The schemas we'll use are a 
// position schema which represents a position of a queen on a board,
// a board schema which represents the state of a board (a sequence of positions)
// and a row schema which is just used to name the columns on the output.
let positionSchema = Schema.Make ["col",IntElement;"row",IntElement] 
let boardSchema = Schema.Make ["board", SequenceElement positionSchema]
let rowSchema = Schema.Make [("row",IntElement);("name",StringElement)]

// create a tuple containing a row, column pair
let inline createPos (row,col) = Tuple.Create positionSchema [Datum.Int col;Datum.Int row] 
// reads row and column from a tuple
let inline readPos tup = tup |> Tuple.Int "row", tup |> Tuple.Int "col"
// reads the board state sequence from a tuple.
let getBoard = Tuple.Tuples "board"
    
// Filter pipeline stage that will keep only positions that do not interfere
// with previously placed queens on the board
let validMoveFilter = 
    // checks to see whether two positions are on the same diagonal 
    let inline shareDiag (row,col) (qrow,qcol) =
        let dr,dc = qrow - row, qcol - col
        dr = dc || dr = -dc    

    let inline queenCanTake (row,col) (qrow,qcol) =
        row = qrow || col = qcol || shareDiag (row,col) (qrow,qcol)        

    let inline isValid (row,col) board = board |> Seq.forall (readPos >> (queenCanTake (row,col)) >> not)
    SimpleFilter (fun tup -> isValid (tup |> readPos) (tup |> getBoard))

// Map stage that adds a position to the board.
// Map stages require two functions ... one to transform tuples
// and another to transform schemas
let acceptMove =
    let mapfun tup =
        let pos = tup |> readPos |> createPos
        let board = tup |> getBoard
        // add this position to the board.
        let newBoard = board.NewSeq board.Schema (Seq.append board.tseq (Seq.singleton pos))
        Tuple.Create boardSchema [newBoard |> Datum.Tuples]
    // Schema function turns input schema into a schema with just a board.
    let schfun _ = boardSchema
    Common.SimpleMap  schfun mapfun

// Each row is going to have its own named tap that generates the sequence
// of possible positions for that row.
let rowTapName row = sprintf "positionGen%d" row 

// Pipeline segment that takes the board from a previous row
// and passes on new boards with added queens for this row.
let computeStage prev row = 
    // start with the board from the previous row
    prev
    // Join with a position generator that will add a new column
    // with all of the possible positions in this row.
    <**> (Join.JoinAsColumn "candidates", (rowTapName row) *> IdentityPipe)
    // Flatten the candidate positions so that we will have a tuple for each
    // candidate.
    <*> Common.Flatten "candidates"
    // print out the number of candidates we've processed.
    <*> PrintStatus (sprintf "Row %d" row)
    // filter out any candidate that could be taken by other 
    // pieces on the board.
    <*> validMoveFilter
    // accept all moves that pass the filter -- add the 
    // position to the board and keep only the board.
    <*> acceptMove    
    
// tap generating helper function.
let createTap name schema seq = (name, seq |> TupleSequence.Create schema |> Tap.FromSeq)

// create a tap representing an empty board.
let emptyBoardTap = 
    [Seq.empty |> TupleSequence.Create positionSchema  |> Datum.Tuples] 
    |> Tuple.Create boardSchema |> Seq.singleton 
    |> createTap "emptyBoard" boardSchema
    
// Main nQueens entry point. Run the n queens problem and output to a sink
let public nQueens sink n : unit =    
    // row numbering scheme. 
    let rowRange = [1..n]
    // For reach row, create a pipe segment and string them together.
    let computePipe = Seq.fold (fun prev row -> computeStage prev row) 
                               ("emptyBoard" *> IdentityPipe) rowRange
                               
    // Tap creation function for a row. Just generates positions 
    // with a known row and for all possible columns
    let positionTapGenerator row =
        seq [for jj in rowRange -> createPos (int64 row,int64 jj)]
        |> createTap (rowTapName row) positionSchema

    // Create a tap that tells us what names to use for rows for printing out the results.
    // These will be the column headers in the output ... i.e. 
    // Row 1, Row 2, Row3, etc.
    let rowNameTap = 
        seq [for ii in 1..n -> [ii |> int64|> Datum.Int; sprintf "Row %d" ii |> Datum.String] 
                               |> Tuple.Create rowSchema]
        |> createTap "rowNames" rowSchema
        
    // We need an empty tap for the board, a tap for each row, and a tap to tell us how to name the
    // columns in the output.
    let taps = emptyBoardTap :: rowNameTap :: [for ii in rowRange -> positionTapGenerator ii]       

    // final pipe to use is the pipe to compute the solutions,
    // plus add a status printer and a stage that will turn 
    // [board:[row,col]] into [row1;row2;row3]... i.e. something that is easier
    // to render in tabular format.    
    let pipe = computePipe
               <*> PrintStatus "Solutions"
               <**> (Join.ExplodeInner ("board",["row"],"col") (["row"],"name"),
                                       "rowNames" *> IdentityPipe)
    // actually run the thing
    Pipes.RunPipes sink pipe taps
