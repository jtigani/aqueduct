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
#light
// This file contains a tutorial for creating Aqueduct pipeline graphs. It starts
// with a trivial example -- reading in a file and writing it out to another file, but
// soon moves up to more complex examples, introducing new pipe stages at each step.

module Sample

open Aqueduct
open Aqueduct.Common
open Aqueduct.Primitives
open Aqueduct.Pipes

open System.Collections.Generic

let createDb (configSetting:string) = 
    let connectionString = System.Configuration.ConfigurationManager.AppSettings.[configSetting]
    Database.DbData.Create(Database.MySql,connectionString)

// creates and runs a simple pipe that will read in a file, print out a status message every 30 seconds
// and write the same pipe to a file. Each row of data is broken up into Tuples, which considt of a Schema and
// a collection of Datum objects.  The schema indicates the type and name of each column, and the Datum has 
// the data. Supported Datum types are Int, Unsigned, Float, String, and Tuples (which is a sequence of tuples).
// When reading in from a delimited file such as a comma or tab-separated file, all fields are read in as strings,
// with the header row as the column labels.
let hello (tap:ITap) (sink:ISink) =
    // a pipe system consists of a tap, a graph of pipe stages, followed by a sink.
    // A set of special operators exists to describe the shape of this graph.
    // All pipe systems start with a name of a tap, followed by the operator "*>".
    // "tapName" *> pipestage can be thought of as "read tap tapName and pipe through pipestages".        
    let stages = 
        "tap"                               // name the beginning of the pipe 'tap'
        *> Common.PrintStatus "hello, pipes!" // follow our tap with a stage that prints "hello, pipes!" every 30 seconds
    
    // Tap the pipe stages, using the tap we created (note this tap name matches the tap name specified in stages)
    // and the sink. Run a single tap to a single sink -- in this case, it will just read in inFile
    // print "hello, pipes" after every 100 rows, and write the file back to outFile
    RunSimplePipe sink stages ("tap",tap) 

// given a file, remove unwanted columns and write out the rest
let dropColumns (tap:ITap) (sink:ISink) (fields:string list)  =

    // normal pipe stages are connected via the <*> operator.  a <*> b can be thought of as "a flows 
    // into b".          
    let stages =  
        "tap" 
        *> Common.PrintStatus "removeColumn"  // print out status updates
        <*> DropColumns fields              // drop the fields we don't want
    
    // run the pipe
    RunSimplePipe sink stages ("tap",tap) 
    
//  This is a slightly more complext pipe that coerces some of the input fields into integer types,
// and adds a column called "sum" representing their sum
let sumColumns (tap:ITap) (sink:ISink)  (fields:string list) = 
    
    // create a schema representing all of the fields we care about. The columns representing the 
    // fields are typed as Ints.
    let schema = Schema.Create (fields |> List.map (fun field -> Column.Make(field,IntElement)))
    
    // function that will get applied to every tuple in the stream to add the 
    // values in fields.
    let sumField tup = 
        List.fold (fun total field -> total + (Tuple.Int field tup)) 0L fields |> Datum.Int
    
    // normal pipe stages are connected via the <*> operator.  a <*> b can be thought of as "a flows 
    // into b".          
    let stages = 
        "tap" 
        *> Common.PrintStatus "sumFields"
        // In case there were any quotes in the input, remove them 
        // (for example, if in a csv you had "1", "2", "3" we'd give just 1,2,3)
        <*> StripQuotes 
        // apply our schema to the data. Note that our schema doesn't have to be exhaustive, just
        // describe the fields that we care about.
        // if we cannot coerce the fields to our schema (e.g. if the value was "foo" it cannot be
        // coerced to a number) then we will throw an exception and the execution will terminate. 
        <*> Common.PartialSchemaApply schema 
        
        // create a new field "sum" representing the sum of the integer fields in the tuple
        <*> Common.AddColumn (Column.Make("sum",IntElement)) sumField
    
    // tap and run the pipe.
    RunSimplePipe sink stages ("tap",tap) 
// here is a slightly more complex version that reads in the file, and filters out
// all tuples that don't match a regular expression on a provided field. 

let regexFilter (tap:ITap) (sink:ISink) (regexField:string) (regexp:string) =     
        
    // To apply the filter, we first create a new column 
    // with the result of the regular expression. We then apply a filte
    // to keep the data points (tuples) that have a non-empty value for that new
    // field. Finally, we drop the added column because it isn't desired in the output.        
        
    let stages = 
        // name the beginning of the pipe 'tap'
        "tap" 
        // print 'regexFilter' status messages
        *> Common.PrintStatus "regexFilter"                 
        // add a column to each tuple that is the result of applying the regular expression
        // to each value in the 'regexField' column.
        <*> Common.AddColumnRegex (regexField,"newField",regexp)         
        // Add a filter that keeps only tuples that had matched the regular expression 
        // i.e the match is non-empty
        <*> Common.FilterColumn "newField" (fun dtm -> dtm <> Datum.StringEmpty)        
        // We don't want to preserve the "newField" in the output, so drop it
        <*>Common.DropColumns ["newField"]        
        
    // tap and run the pipe.
    RunSimplePipe sink stages ("tap",tap) 
        
[<EntryPoint>]
let main (args:string array) =
    
    let printusage () = 
        printfn "Usage:"
        printfn "   hello infile outfile"
        printfn "   dropColumns infile outfile [field1,field2..]"
        printfn "   regexFilter infile outfile field regex "       
        printfn "   sumColumns infile outfile [field1,field2..]"
        printfn "   downloadStocks ticker1[,ticker2,...]"
        printfn "   calcStocks ticker1[,ticker2,...] startDate endDate outfile"
        printfn "   nQueens outfile n"
        printfn "   sudoku infile"

    // helper function to create a tap that will pick a format based on the file name
    // csv,tab,bin,xml,etc
    let genTap fname = IO.readFromFile (IO.formatFromFileName fname) fname
    // helper function to create a sink with a format based on the file name
    // csv,tab,bin,xml,etc
    let genSink fname = IO.writeToFile (IO.formatFromFileName fname) fname
    
    let splitNames (fields:string)= (fields.Split([|','|]) |> List.ofArray)
    
    match args with    
    | [|"hello";infile;outfile|]  ->  hello (genTap infile) (genSink outfile)    
    | [|"dropColumns";infile;outfile;fields|]  ->  dropColumns (genTap infile) (genSink outfile) (splitNames fields) 
    | [|"sumColumns";infile;outfile;fields|]  ->  sumColumns (genTap infile) (genSink outfile) (splitNames fields) 
    | [|"regexFilter";infile;outfile;field;regex|]  ->  regexFilter (genTap infile) (genSink outfile) field regex
    | [|"calcStocks";ticker;startDate;endDate;outfile|]  ->  
        let tickers = ticker .Split ( [|','|] ) |> List.ofArray
        Stock.calcStocks tickers (startDate,endDate) (genSink outfile)
    | [|"downloadStocks";ticker|]  ->  
        let tickers = ticker .Split ( [|','|] ) |> List.ofArray
        Stock.downloadStocks tickers
    | [|"sudoku";infile|]  -> Sudoku.sudoku (genTap infile)
    | [|"nQueens";outfile;n|]  ->n |> int |> NQueens.nQueens (genSink outfile)
    | _ -> printfn "Invalid arguments: %A" args;  printusage (); exit 1;
    
    0
