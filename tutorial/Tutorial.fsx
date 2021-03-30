#r "bin\\debug\\Aqueduct.dll"
open Aqueduct
open Aqueduct.Primitives
open Aqueduct.Pipes
open Aqueduct.Common;;

// create a simple pipe and run it
let helloPipe = "hello" *> Common.PrintStatus "Hello, Aqueduct"
let sink = IO.writeToFileAuto "output.tab"
let tap = IO.readFromFileAuto "sample.csv.gz"

RunSimplePipe sink helloPipe ("hello",tap) ;;

// ok now we'll create a pipe that adds a field summing the folumns "foo" and "bar" and adding it to 
// the column "sum"

// create a schema representing all of the fields we care about. The columns representing the 
// fields are typed as Ints.
let fields = ["foo";"bar"]
let schema = Schema.Create (fields |> List.map (fun field -> Column.Make(field,IntElement)))


// function that will get applied to every tuple in the stream to add the 
// values in fields.
let sumField tup = fields |> List.map (fun field -> Tuple.Int field tup) |> List.sum |> Datum.Int
    
// normal pipe stages are connected via the <*> operator.  a <*> b can be thought of as "a flows 
// into b".          
let sumStages = 
    "sum" 
    *> Common.PrintStatus "sumFields"
    // apply our schema to the data. Note that our schema doesn't have to be exhaustive, just
    // describe the fields that we care about.
    // if we cannot coerce the fields to our schema (e.g. if the value was "foo" it cannot be
    // coerced to a number) then we will throw an exception and the execution will terminate. 
    <*> Common.PartialSchemaApply schema 
    
    // create a new field "sum" representing the sum of the integer fields in the tuple
    <*> Common.AddColumn (Column.Make("sum",IntElement)) sumField
    
// write the result to "sum.tab"
let sumSink = IO.writeToFileAuto "sum.tab"    
// tap and run the pipe. 
RunSimplePipe sumSink sumStages ("sum",tap) ;;

// This is the reusable pipe example. Let's say we want to 
// filter out any rows that don't match some regular expression. This requires 
// three steps -- add a new column with the result of the regular expression, filter
// out rows where that column is empty, then drop the new column. 
// This is something we might want to do more than once -- so we can create a 
// function encapsulating the pattern
let regexFilter (regexField,regexp) lhs =         
        // lhs is the left hand side of the pipe -- since we're going to be
        // adding the filter into the middle of a pipeline.
        lhs         
        // add a column to each tuple that is the result of applying the regular expression
        // to each value in the 'regexField' column.
        <*> Common.AddColumnRegex (regexField,"newField",regexp)         
        // Add a filter that keeps only tuples that had matched the regular expression 
        // i.e the match is non-empty
        <*> Common.FilterColumn "newField" (fun dtm -> dtm <> Datum.StringEmpty)        
        // We don't want to preserve the "newField" in the output, so drop it
        <*>Common.DropColumns ["newField"]        
        
// in this hypothetical example, we're going to use our new regexFilter twice --
// once to only keep rows where field foo begins with a 100, and another to only keep
// rows where field "bar" has an odd digit as the third character
let filterStages = 
    "tap" 
    *> Common.PrintStatus "regexFilterPre"
    |> regexFilter ("foo","^100")
    |> regexFilter ("bar","^[0-9]*[13579]$")
    <*> Common.PrintStatus "regexFilterPost"
    
let filterSink = IO.writeToFileAuto "filter.tab"
// tap and run the pipe.
RunSimplePipe filterSink filterStages ("tap",tap) 

//
// Begin stock download pipe
// these are the stock tickers we care about
let tickers = ["msft";"yhoo";"goog";"gm";"t"]

let stockUrl = sprintf @"http://ichart.finance.yahoo.com/table.csv?s=%s&ignore=.csv"
let taps = tickers 
           |> List.map stockUrl 
           |> List.map (IO.readFromWeb (IO.CsvFormatInfo None))
           |> List.zip tickers
let tickerFile ticker = ticker + ".tab"
let pipeGen ticker = ticker *> PrintStatus ("Downloading " + ticker)
let pipes = tickers |> List.map pipeGen
let sinks = tickers |> List.map tickerFile |> List.map IO.writeToFileAuto 
Pipes.MultiRunPipes (List.zip sinks pipes) taps;;

// begin stock analysis pipe (exects already downloaded stock history)

let deltaFun (tup:Tuple) : Datum = 
    let openPrice = Tuple.Float "Open" tup
    let closePrice = Tuple.Float "Close" tup
    let delta = 100. * (openPrice - closePrice) / openPrice
    delta |> Datum.Float ;;


let trailingAverage count name =
    let schemaFun (sch:Schema)= Column.Make (name,FloatElement) |> sch.Add
    let windowFun (sch:Schema) (tups:Tuple array) = 
        let avg = tups |> Array.averageBy (fun tup -> Tuple.Float "Close" tup) 
        Tuple.Create sch ((Datum.Float avg) :: tups.[0].values)
    SlidingWindow count windowFun schemaFun;;

let startDate = "1/1/2000"
let endDate = "1/1/2008"
let startDay,endDay = startDate |> System.DateTime.Parse,endDate |> System.DateTime.Parse

let dateFilter date = 
    let day = date |> Datum.ExtractString |> System.DateTime.Parse
    (day >= startDay && day < endDay);;

let schema = Schema.Make [("Open",FloatElement);("Close",FloatElement)];;

let stockPipe (ticker:string) : PipeStage = 
    let avg30Field = ticker + "_30"
    let avg5Field = ticker + "_5"
    let pctChangeField = ticker + "_pct_change" 

    ticker
    *> PrintStatus (sprintf "Reading stock %s" ticker)
    <*> FilterColumn "Date" dateFilter 
    <*> PartialSchemaApply(schema)
    <*> ExpectOrdering (SchemaKey.Create ([Descending,"Date"],true))                                                        
    <*> AddColumn (Column.Make(pctChangeField,FloatElement)) deltaFun                                           
    <*> trailingAverage 30 avg30Field
    <*> trailingAverage 5 avg5Field
    <*> KeepColumns ["Date";pctChangeField;avg30Field;avg5Field]

let pipes = tickers |> List.map stockPipe ;;

let reduceFun lhs rhs = 
    lhs <**> (Join.Join (JoinOrdered,JoinFullOuter) ["Date"] ["Date"],rhs)

let combinedPipes = 
    pipes |> List.reduce reduceFun
    <*> PrintStatus "Stocks computed"
    <*> ReorderColumnsBy (fun (sch:Schema) col -> 
                               if col = "Date" then -1 else sch.NameIndex col)
;;

let taps = tickers |> List.map tickerFile |> List.map IO.readFromFileAuto
let stockSink = IO.writeToFileAuto "stocks.tab"
Pipes.RunPipes stockSink combinedPipes (List.zip tickers taps);;
