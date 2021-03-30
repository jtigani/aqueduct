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

// This sample downloads stocks from yahoo finance, computes the percent daily change, as well
// as the 5 and 30-day moving averages and writes the results to a sink.
// Some nice things: download and calculation are done in paralell where possible,
// and because the inputs and outputs are streamed, you don't have to worry about 
// things fitting into memory.
module Stock

// Example usage:
// sample.exe downloadStocks t,msft,goog,yhoo
// sample.exe calcStocks msft,goog,t 1/1/2000 1/1/2008 stocks.tab
open Aqueduct
open Aqueduct.Common
open Aqueduct.Primitives
open Aqueduct.Pipes

// URL to download stock history from Yahoo! finance. The %s gets filled in by
// the ticker string
let stockUrl = sprintf @"http://ichart.finance.yahoo.com/table.csv?s=%s&ignore=.csv"

// File name to store downloaded stock data. We use format detection based
// on file extension --
// .csv is comma separated, .tab is tab separated, .bin is a binary format,
// .xml is xml. Add .gz to for gzip compression (as in .csv.gz)
let tickerFile ticker = ticker + ".tab"

// Given a list of tickers, download stock history for them
// in parallel.
let public downloadStocks (tickers:string list) : unit =
    // Generate the taps, which specify where the data is coming from. In this case, we're
    // reading it from yahoo finance website.
    let taps = tickers |> List.map (fun ticker -> ticker, IO.readFromWeb (IO.CsvFormatInfo None) (stockUrl ticker))
    // The pipes are going to be very simple -- just a name and a status printer.
    let pipes = tickers |> List.map (fun ticker -> ticker *> PrintStatus ("Downloading " + ticker))
    // Sinks just write to file based on the ticker's name
    let sinks = tickers |> List.map tickerFile |> List.map IO.writeToFileAuto 
    // run pipes in parallel. 
    Pipes.MultiRunPipes (List.zip sinks pipes) taps

// After downloading the data, we can start to do interesting things with it.
// this pipe filter based on a date range, then will compute the % difference between open and close 
// prices for the stock.
let stockPipe ((startDate:string),(endDate:string)) (ticker:string) : PipeStage = 
    // precompute the beginning and ending date time we want to use for the filter
    let startDay,endDay = startDate |> System.DateTime.Parse,endDate |> System.DateTime.Parse
    // date filter function. We're going to only keep days that are beteen our start
    // and end dates
    let dateFilter date = 
        let day = date |> Datum.ExtractString |> System.DateTime.Parse
        (day >= startDay && day < endDay)
    // We want to use Open and Close as floating point values, so create a schema for them
    // so that they will be coerced. Note that we could do the coercion ourselves, but this
    // way if we use the open and close prices in other calculations, we don't have
    // to coerce them multiple times.
    let schema = Schema.Make [("Open",FloatElement);("Close",FloatElement)]
    
    // Function that computes the percent change between the Open and Close prices
    let deltaFun (tup:Tuple) : Datum = 
        let openPrice = Tuple.Float "Open" tup     // read the Open field from the tuple
        let closePrice = Tuple.Float "Close" tup   // read the Close field from the tuple
        let delta = 100. * (openPrice - closePrice) / openPrice // compute the % change
        delta |> Datum.Float 
    
    // create a pipe stage that will compute an N-day rolling
    // average.
    let trailingAverage count name =
        // the resulting schema is going to be the input schema with the addition of a new
        // average column
        let schemaFun (sch:Schema)= Column.Make (name,FloatElement) |> sch.Add
        // Function that gets called for every sliding window of tuples.
        let windowFun (sch:Schema) (tups:Tuple array) = 
            let avg = tups |> Array.averageBy (fun tup -> Tuple.Float "Close" tup) |> Datum.Float
            Tuple.Create sch (avg :: tups.[0].values)
        SlidingWindow count windowFun schemaFun
    let avg30Field = ticker + "_30"
    let avg5Field = ticker + "_5"
    let pctChangeField = ticker + "_pct_change" 
    // This is the pipeline. It starts with the ticker name which is how we know
    // to hook it up to the ticker tap
    ticker
    *> PrintStatus (sprintf "Reading stock %s" ticker) // status printer stage
    <*> Common.FilterColumn "Date" dateFilter          // Filter based on date range
    <*> PartialSchemaApply(schema)                     // coerce Open and Close to float values
    <*> ExpectOrdering (SchemaKey.Create ([Descending,"Date"],true)) // Indicate we expect these
                                                       // to be in reverse chronological order
    <*> AddColumn (Column.Make(pctChangeField,FloatElement)) deltaFun  // Compute the percent change, add 
                                                      // it as a column with the ticker name
    <*> trailingAverage 30 avg30Field
    <*> trailingAverage 5 avg5Field
    <*> KeepColumns ["Date";pctChangeField;avg30Field;avg5Field] // only keep the date and the new columns
    
// This creates a single pipe that joins a number of stock pipes together
let combinePipes (pipes:PipeStage list) : PipeStage = 
    // reduce function that will join all of the pipes on their date field. Because they are all
    // ordered the same way, we can do the join as a stream.
    let reduceFun lhs rhs = lhs <**> (Join.Join (JoinOrdered,JoinFullOuter) ["Date"] ["Date"],rhs)
    // after the join the date column will be the second column... move it to the first column
    let reorderFun (sch:Schema) (col:string) = if col = "Date" then -1 else sch.NameIndex col
    // create the combiner pipe
    pipes |> List.reduce reduceFun
    <*> Common.ReorderColumnsBy reorderFun // Move the date column to the beginning
    <*> PrintStatus "Stocks computed"      // output status



// Given a list of stock tickers (for already downloaded stock data) and a start date and end date
let public calcStocks (tickers:string list) (startDate:string,endDate:string) (sink:ISink) : unit =
    // create taps from tickers ... we expect them to be in already downloaded files
    let taps = tickers |> List.map (fun ticker -> ticker, IO.readFromFileAuto (tickerFile ticker))
    // create the stock pipes
    let pipes = tickers |> List.map (stockPipe (startDate,endDate))
    // combine the pipes to a single one that will have all of the tickers
    let pipe = pipes |> combinePipes
    // run the pipes to our sink
    Pipes.RunPipes sink pipe taps
    