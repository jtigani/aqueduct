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

// This sample is a sudoku solver. 
// The input is an initial sudoku board with the format
// row,col,value
// 1,1,8
// 1,2,3
// 2,3,6
// 2,5,3
// ...
// (this is for an example input of a csv file. Check out sudo1.csv for input for
// a valid sudoku board)

module Sudoku

open Aqueduct
open Aqueduct.Common
open Aqueduct.Primitives
open Aqueduct.Pipes

// an entry is the triple of row,col,and value which describes something that is filled
// in on the sudoku board
let entrySchema = Schema.Make ["row",IntElement;"col",IntElement;"value",IntElement] 
let boardSchema = Schema.Make ["board", SequenceElement entrySchema]

// From a triple row,col,value, create an entry Tuple
let inline createEntry (row,col,value) = 
    Tuple.Create entrySchema [Datum.Int row;Datum.Int col;Datum.Int value] 

// from a Tuple containing an entry, create a row,col,value triple
let inline readEntry tup = tup |> Tuple.Int "row", tup |> Tuple.Int "col",tup |> Tuple.Int "value"
let getBoard = Tuple.Tuples "board"

// filter that decides whether a sudoku move is valid given a board.
let validMoveFilter =     
    let inline box n = (n-1L)/3L
    let inline checkPosition (row,col,value) (row2,col2,value2) =        
        value = value2 && (row = row2 || col = col2 || (box row = box row2 && box col = box col2))
    // a position is valid when there are no collisions with either
    // the row, column or box with any position on the board
    let isValid entry board =  
        board |> Seq.forall (readEntry >> checkPosition entry >> not)

    SimpleFilter (fun tup -> isValid (tup |> readEntry) (tup |> getBoard))

// This is a map stage that adds an entry to a board.
// it turns [rowK,colK,valueK,board:[row1,col1,value1;row2;col2,value2;...]] into
//          [board:[row1,col1,value1;row2;col2,value2;...;rowK,colK,valueK]]
let acceptMove =
    let mapfun tup =
        let entrySeq = tup |> readEntry |> createEntry |> Seq.singleton
        let board = tup |> getBoard
        let newBoard = board.NewSeq board.Schema (Seq.append board.tseq entrySeq)
        Tuple.Create boardSchema [newBoard |> Datum.Tuples]
    let schfun _ = boardSchema
    Common.SimpleMap  schfun mapfun

// name of tap that has the available values for this cell
let posTapName row col = sprintf "positionGen%d.%d" row col 

// creates a compute stage for a particular cell. Will take a stream
// of valid boards, try all combinations of values for this cell and
// pass along the boards with those valid combinations added.
let computeStage prev row col =     
    prev        
    <**> (Join.JoinAsColumn "candidates", (posTapName row col) *> IdentityPipe)
    <*> Common.Flatten "candidates"    
    <*> validMoveFilter
    <*> acceptMove   
    <*> PrintStatus (sprintf "Position (%d,%d)" row col)

let createTap name schema seq = (name, seq |> TupleSequence.Create schema |> Tap.FromSeq)
    
let public sudoku boardTap: unit = 
    // precomputed ranges specifying the ranges of values in the sudoku.
    // note that the rows and columns are 1-biased.       
    let rowRange,colRange,valueRange = [1..9],[1..9],[1..9]
    // range of pairs of row,column values
    let rcRange =  [for row in rowRange -> [for col in colRange -> (row,col)]] |> List.concat
        
    let initialBoardPipe = "initialBoard" *> SchemaApply entrySchema
    
    // Each cell in the 9x9 sudoku board is going to get its own compute stage. However, we want to 
    // filter out all cells that are already filled in, since no work needs to be done in those
    // cells.
    let initialBoardSink = new CollectorSink ()
    Pipes.RunSimplePipe initialBoardSink initialBoardPipe ("initialBoard",boardTap)
    let initialBoard = initialBoardSink.GetSeq |> Seq.map readEntry
    let boardContains (r,c) = initialBoard |> Seq.tryFind (fun (r1,c1,_) -> int64 r=r1 && int64 c = c1) <> None     
    
    // build the compute pipe. For each row,column pair that isn't in the initial board,
    // we add a compute stage. 
    let computePipe = 
        rcRange
        |> List.filter (boardContains >> not)
        |> Seq.fold (fun prev (row,col) -> computeStage prev row col)
                    (initialBoardPipe <*> Join.Group JoinSmall [] "board")
    
    // prints a sudoku board to stdout             
    let printBoard tseq = 
        let board = Array2D.zeroCreate 10 10   
        for tup in tseq do
            let row,col,value = readEntry tup
            board.[int row,int col] <- int value
        
        for row in rowRange do
            if row % 3 = 1 then printfn ("-------------------")
            printf("|")
            for col in colRange do                
                if board.[row,col] = 0 then
                    printf " " 
                else
                    printf "%d" board.[row,col]
                if col % 3 = 0 then printf ("|")        
                else printf (" ")    
            printfn ""
        printfn ("-------------------")
        ()
        
    printfn "Initial board:"
    initialBoardSink.GetSeq |> printBoard
    
    let sink = new CollectorSink ()
        
    // Given a row and a column generates one tuple for each value the cell can have
    // in the form [row,col,value]
    let tapGenerator (row,col) =
        seq [for jj in valueRange -> createEntry (int64 row,int64 col, int64 jj)]
        |> createTap (posTapName row col) entrySchema
    let entryTaps = rcRange |> List.map tapGenerator
                
    let taps = ("initialBoard", boardTap) :: entryTaps
               
    // actually run the thing
    Pipes.RunPipes sink computePipe taps
    
    // print the results
    sink.GetSeq |> Seq.head |> getBoard |> printBoard