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

namespace Aqueduct

(* This module implements reading and writing to csv *)
module internal CsvIO =    
    open Aqueduct.Primitives
    open Aqueduct.Pipes
    open Aqueduct.Common
    

/// File loading:    
        
    let  LineCountingReader(stream: System.IO.Stream) =
        let lineNumber = ref 0
        {new System.IO.StreamReader(stream) with
                
            member x.ReadLine() =
                let l = base.ReadLine()
                lineNumber := !lineNumber + 1
                l
        },(fun () -> !lineNumber)
        
    
    exception ParsingError of string
    
    let private defaultSchema names =
        List.map (fun n -> Column.Make(n, Primitives.StringElement)) names |> Primitives.Schema.Create
                    
    let private readCsvHeader sep (reader: System.IO.TextReader)   =                
        let line = reader.ReadLine()
        let names = line.Split([|sep|]) |> List.ofArray
        let cleanNames = names |> List.map (fun s -> s.Trim())  |> List.map stripQuotes
        cleanNames |> defaultSchema                        
                                
    
    
    let private tupleFromLine sep (stringSchema:Primitives.Schema) =
        let schLen = List.length stringSchema.Elements       
        fun (line:string) ->
            let lineSplit = splitLine sep line  
            let splitLen = List.length lineSplit            
            if (splitLen <> schLen) then
                raise <| ParsingError (sprintf "wrong number of columns (%d vs %d): %s" splitLen schLen line)
            else 
                let data = [for s in lineSplit do yield Datum.String s]            
                Tuple.Create stringSchema data
        
    let processLineAsync buffer isDone (reader:System.IO.TextReader) schema =    ()

    let public readTuplesFromSep sep (knownSchema: Schema option) (stream: System.IO.Stream) : Primitives.TupleSequence =
        let reader,getLine = LineCountingReader(stream)
        let stringSchema= 
                match knownSchema with 
                | None -> readCsvHeader sep reader
                | Some sch -> sch.Names |> List.map (fun n -> n,StringElement) |> Schema.Make
        let schema = match knownSchema with | None -> stringSchema | Some sch -> sch
        let mapFun = tupleFromLine sep stringSchema
        let tseq = seq {                                                    
                let isDone = ref false            
                while ( (!isDone) = false) do                    
                    let line = reader.ReadLine ()
                    if (line = null) then isDone := true else
                    let t = try mapFun line |> Some with
                            ParsingError s -> fprintfn System.Console.Error "%d: %s" (getLine ()) s
                                              None
                    match t with
                    | Some tup -> 
                        yield (if knownSchema <> None then Tuple.CoerceToSchema schema tup else tup)
                    | None -> ()                    
                reader.Close ()
            }
           
        tseq |> TupleSequence.Create schema
        
    let public readTuplesFromCsv k s = readTuplesFromSep ',' k s
    let public readTuplesFromTab k s = readTuplesFromSep '\t' k s
                       
    exception UnsupportedElementType of SchemaElement * string
             
    let private genElement (element:Primitives.SchemaElement) name : string =
        let elementName = match name with "" -> "Value" | _ -> name
        match element with 
        | Primitives.SequenceElement _ -> raise <| UnsupportedElementType (element, name) 
        | _ -> elementName
    
    exception UnsupportedDatumType of Datum * string
    
    let private genDatum (datum:Datum) name : string =
        match (datum) with
        | IntDatum i -> string i
        | UnsignedDatum u -> string u
        | FloatDatum f -> string f
        | StringDatum s -> s
        | TuplesDatum ts -> raise <| UnsupportedDatumType (datum, name)
        | _ -> failwith (sprintf "Cannot generate string for datum %A in CVS format" datum)
        
    let private genLine sep (schema:Schema) things gen =
        let names = schema.Names |> List.mapi (fun i n -> (i,n)) |> Map.ofList        
        let quote (str:string) = if str.Contains("\"") then "'" + str + "'" else "\"" + str + "\""
        let prepThing (str:string) = 
            if str.Contains(sep) || str.Contains("\"") || str.Contains ("'") then quote str else str
        let thingArray = List.mapi (fun index element -> gen element (Map.find index names)) things |>
                         List.map prepThing |> List.toArray
                    
        System.String.Join (sep, thingArray)
        
    let private genSchema sep (schema:Schema) =
        genLine sep schema schema.Elements genElement
        
    let private genTuple sep (tup:Tuple) =
        genLine sep tup.schema tup.values genDatum
        
    (* Writes data as comma-separated values. *)
    let private writeTuples (sep:string) (stream: System.IO.Stream) (tseq: Primitives.TupleSequence) =        
        let writer = new System.IO.StreamWriter(stream) :> System.IO.TextWriter
        genSchema sep tseq.Schema |> writer.WriteLine
        for tup in tseq.tseq do        
            genTuple sep tup |> writer.WriteLine                                 
            writer.Flush ()
    
    let public writeTuplesAsTab(stream: System.IO.Stream) (tseq: Primitives.TupleSequence) =  
        writeTuples "\t" stream tseq
        
    (* Writes data as comma-separated values. *)
    let public writeTuplesAsCsv (stream: System.IO.Stream) (tseq: Primitives.TupleSequence) =        
        writeTuples "," stream tseq        