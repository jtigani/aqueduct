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


/// <summary>
/// The Aqueduct.Common module contains a number of high-level pipeline operations
/// that make writing pipelines easier. Operations to add and remove columns, transform them, operate on
/// inner columns (i.e. columns within a column), sorting, and more are provided.
/// </summary>
module public Common =    
    open Aqueduct.Primitives
    open Aqueduct.Pipes
    open Aqueduct.Internal
    
    type private Quoted = NoQ | SingleQ | DoubleQ
    
    /// <summary>Adds a new column by mapping an existing row tuples to new data.</summary>
    /// <param name="name">The name of map operation.</param>             
    /// <param name="constituent">The <see cref="Column" /> of the new column.</param>
    /// <param name="f">A function taking a <see cref="Tuple" /> as input and returning a <see cref="Datum" />.</param> 
    let SimpleMapAdd (constituent:Column) (f : (Tuple -> Datum))  = MapAddPipe.Create(sprintf "SimpleMapAdd(%A)" constituent.name,f,(fun _ -> constituent))
    let SimpleMap (schgen:Schema->Schema) (f : (Tuple -> Tuple))  = MapPipe.Create(sprintf "SimpleMap" ,f,(fun x->x),schgen)

    /// Pipe that just faithfully pipes its input to its output 
    let IdentityPipe = {new IPipe with
                        member o.Name () = "Identity"
                        member o.SchemaEffect sch = sch
                        member o.Run tseq f = f tseq}            
           
    let internal stripQuotes (str:string) = 
        if (str.Length >= 2 && str.StartsWith("\"") && str.EndsWith("\"")) then 
            str.Substring(1,str.Length - 2) 
        else if (str.Length >= 2 && str.StartsWith("'") && str.EndsWith("'")) then 
            str.Substring(1,str.Length - 2) 
        else str
         
    let internal splitLine delim (input:string) =       
        let enum = input.GetEnumerator ()                
        let gen start count acc = input.Substring(start,count) :: acc
        
        let rec splitFun (startIndex,count,acc) =         
            if (not <| enum.MoveNext ()) then gen startIndex count acc |> List.rev else 
            let ch = enum.Current
            let nextCount = count + 1
            match (ch) with             
            | '\'' when count = 0-> skipToQuote (SingleQ, startIndex, nextCount,acc)  // beginning of single quote
            | '\"' when count = 0-> skipToQuote (DoubleQ, startIndex, nextCount,acc)  // beginning of double quote
            | ch when ch = delim -> splitFun (startIndex + count + 1, 0,gen startIndex count acc) // split here
            | _  -> splitFun (startIndex, nextCount,acc) 
        and skipToQuote (inquotes,startIndex,count,acc) =
            if (not <| enum.MoveNext ()) then gen startIndex count acc |> List.rev else 
            let ch = enum.Current
            let nextCount = count + 1
            match (ch,inquotes) with 
            | ('\'', SingleQ) -> splitFun (startIndex, nextCount,acc) // end of single quote
            | ('\"',DoubleQ) ->  splitFun (startIndex, nextCount,acc) // end of double quote
            | _  -> skipToQuote(inquotes,startIndex, nextCount,acc) 
            
             
        splitFun (0,0,[])
        
    let internal lineSplitSchemaGen ncols =
        fun schema ->
            let columns = [for i in 0..(ncols - 1) do yield Column.Make( "$" + string i, StringElement)]
            Schema.Create columns
                
    /// Map function for split a line based on a delimiter 
    let public lineSplitterMap(ncols,delim:char) = 
        let schema = lineSplitSchemaGen ncols Schema.Empty
        let mapfun (tuple:Tuple) =
            let input = (tuple.GetIndexed 0).asString
            let vals = splitLine delim input |> List.map (fun v -> Datum.String v)
            // pad to schema size
            let vals' = 
                if List.length vals < ncols then 
                    vals @ (List.init (ncols - List.length vals)(fun _ -> Datum.StringEmpty)) else
                    vals
            Tuple.Create schema vals'            
        mapfun 

                        
    /// Pipe that will split a line (which should contain one string field)
    /// on a certain delimiter. You must know how many columns you expect, however.
    let LineSplitter(delim:char,ncols:int) = MapPipe.Create(sprintf "LineSplitter(%A)" (string delim),
                                                                lineSplitterMap (ncols,delim),reordering,
                                                                lineSplitSchemaGen ncols)
            
    /// Pipe splitting a line on commas 
    let SplitLineOnComma n = LineSplitter(',',n)
    /// Pipe splitting a line on tabs
    let SplitLineOnTab n = LineSplitter('\t',n) 
        
    /// Schema transformation function indicating the schema doesn't change. (Identity function for schemas)
    let schemaPreserving = fun sch -> sch
    
    /// Pipe stage removing whitespace from all string datums
    let  TrimWhitespace = 
        let tupleMapFun (comp:Column) (datum:Datum) =
            match comp.element with StringElement -> Primitives.Datum.String(datum.asString.Trim()) | _ -> datum
        let mapfun (input:Tuple) = Tuple.Create input.schema  (List.map2 tupleMapFun input.schema.Columns input.values)
        MapPipe.Create("TrimWhitespace",mapfun,orderPreserving,schemaPreserving)
    
    /// Pipe stage trimming matching initial and trailing quotes from all string tuples 
    let StripQuotes = 
        let tupleMapFun (comp:Column) (datum:Datum) =
            match comp.element with StringElement -> Primitives.Datum.String(stripQuotes datum.asString) | _ -> datum
        let mapfun (input:Tuple) = Tuple.Create input.schema  (List.map2 tupleMapFun input.schema.Columns input.values)
        MapPipe.Create ("StripQuotes",mapfun,orderPreserving,schemaPreserving)
    
    /// Pipe cleaning flags. None is run no cleanup
    type CleanupInputFlags = CleanupNone = 0 | CleanupTrimWhitespace = 1 | CleanupStripQuotes = 2 | CleanupAll = 0xFF
    
    let CleanupInput (cleanup:Lazy<CleanupInputFlags>) =
        let tupleMapFun (comp:Column) (datum:Datum) =
            match comp.element with
            | StringElement -> let flags = cleanup.Value
                               let s = datum.asString
                               let s' = 
                                    match flags with
                                    | CleanupInputFlags.CleanupNone  -> s
                                    | CleanupInputFlags.CleanupTrimWhitespace -> s.Trim ()
                                    | CleanupInputFlags.CleanupStripQuotes -> stripQuotes s
                                    | CleanupInputFlags.CleanupAll -> s.Trim () |> stripQuotes
                                    | _ -> failwith "unexpected"
                               
                               Primitives.Datum.String(s')
            | _ -> datum
        let mapfun (input:Tuple) = Tuple.Create input.schema  (List.map2 tupleMapFun input.schema.Columns input.values)
        MapPipe.Create ("CleanupInput",mapfun,orderPreserving,schemaPreserving)
        
    /// <summary>Applies a schema to the current pipeline. The whole pipeline is coerced to the given
    /// schema.</summary>
    /// <param name="schema">The schema to apply.</param>
    /// <exception cref="SchemaValidation">Thrown if the schema is incompatible with the pipeline.</exception>
    let SchemaApply (schema:Schema) =
        let mapfun sch (input:Primitives.Tuple) =
            Tuple.CoerceToSchema sch input
        let schemaFun (sch:Schema) = schema
        FastMapPipe.Create ("SchemaApply",(mapfun), schemaFun)
               
    /// <summary>Applies a partial schema to the current pipeline. Each pipeline schema component whose
    /// name matches a name of a component in the given schema is coerced to that component.</summary>
    /// <param name="schema">The schema to apply.</param>
    /// <exception cref="SchemaValidation">Thrown if the schema is incompatible with the pipeline.</exception>
    let PartialSchemaApply (schema:Schema) =
        let partialApplyMapFun (finalSchema:Schema) (input:Primitives.Tuple) =
            Tuple.CoerceToSchema finalSchema input
        let schemaFun (sch:Schema) = schema.Columns  |> List.map (fun comp -> (comp.name, comp)) |> sch.Update
        FastMapPipe.Create ("PartialSchemaApply",partialApplyMapFun, schemaFun)
       
    /// <summary>Applies a partial schema to the current pipeline. Each pipeline schema component whose
    /// name matches a name of a component in the given schema is coerced to that component. If a component
    /// of the given schema cannot be matched with the pipeline, it is ignored.</summary>
    /// <param name="schema">The schema to apply.</param>
    let PartialSchemaApplyIfExists (schema:Schema) =
        let schemaFilter (inputSchema:Schema) =
            schema.Columns |> List.filter (fun comp -> inputSchema.ContainsColumn comp.name)
        let partialSchemaApplyIfExistsMapFun (finalSch:Schema) (input:Primitives.Tuple) =
            let mapfun (comp0:Column) (comp1:Column) (dtm:Datum) =
                dtm.Coerce (comp0.element,comp1.element)

            let newValues = List.map3 mapfun input.schema.Columns finalSch.Columns input.values
            Tuple.Create finalSch newValues
        let schemaFun (sch:Schema) = schema.Columns  |> List.map (fun comp -> (comp.name, comp)) |> sch.Update
        FastMapPipe.Create ("PartialSchemaApplyIfExists",partialSchemaApplyIfExistsMapFun ,schemaFun)

    /// <summary>Converts the content of columns to lower case using the invariant culture.</summary>
    /// <param name="names">The list of column names.</param>
    let ToLower names =
        let toLowerMapFun (input:Primitives.Tuple) =                        
            let lowered = names |> List.map (fun n ->(n, (input.GetString n).ToLowerInvariant() |> Primitives.Datum.String))
            input.UpdateValues lowered                 
        let orderFun ord = reorderSome (fun field -> List.exists (fun n -> n = field) names) ord
        MapPipe.Create ("ToLower",toLowerMapFun,orderFun,schemaPreserving)
        
    /// <summary>Drops columns from the pipeline.</summary>
    /// <param name="names">The list of column names.</param>
    let DropColumns names =
        let mapfun (oschema:Primitives.Schema) (input:Primitives.Tuple) = oschema.SelectColumns input
        let schemafun (input:Primitives.Schema) = input.DropColumns names                        
        FastMapPipe.Create (sprintf "DropColumns(%A)" names,mapfun,schemafun)
        
    /// <summary>Only keep given columns in the pipeline.</summary>
    /// <param name="names">The list of column names.</param>
    let KeepColumns names =
        let mapfun (oschema:Primitives.Schema) (input:Primitives.Tuple) = oschema.SelectColumns input
        let schemafun (input:Primitives.Schema) = input.KeepColumns names                
        FastMapPipe.Create (sprintf "KeepColumns(%A)" names,mapfun,schemafun)
 
    /// <summary>Reorder columns in the pipeline. New indices are relative, meaning that they do not have to
    /// directly match the original indices range but that their relative order is used to decide the reordering.
    /// For example, if a schema has columns named "a," "b" and "c" in that order and the reordering function
    /// returns 1 for "a", 7 for "b" and 2 for "c" the new order will be "a," "c" and "b."</summary>    
    /// <param name="f">The function to reorder the columns. The function takes the pipeline <see cref="Schema" /> and
    /// the name of a column as input and returns a relative index for that column in the pipeline.</param>   
    let ReorderColumnsBy (f:Schema->string->int) =
        let mapfun (oschema:Primitives.Schema) (input:Primitives.Tuple) = oschema.SelectColumns input            
        let schemafun (input:Primitives.Schema) = 
            let indexMap = input.Names |> List.map (fun n -> f input n, input.NameIndex n)
                                       |> List.sortBy (fun (r,_) -> r) |> List.mapi (fun i (_,o) -> (i,o)) |> Map.ofList
            let dump =  input.Columns |> Array.ofList
            let components = List.init (Array.length dump) (fun idx -> dump.[Map.find idx indexMap]) 
            Schema.Create(components,input.Key)
        FastMapPipe.Create ("ReorderColumns",mapfun,schemafun)
    
    /// <summary>Adds columns by filtering an existing string column with a regular expression pattern.
    /// The contents of each new column are either the corresponding match group of the matching string, or the empty string
    /// if there is no such group.</summary>
    /// <example><c>AddColumnsRegex ("url",["scheme"; "rest"],"^(?<scheme>[a-z0-9]*:)(?<rest>.*)")</c></example>
    /// <param name="fromName">The name of the column to filter.</param>
    /// <param name="toName"s>The list of names of columns to add.</param>
    /// <param name="pattern">The regular expression pattern.</param>                 
    let AddColumnsRegex (fromName,toNames,pattern) = 
        let regex:System.Text.RegularExpressions.Regex = new System.Text.RegularExpressions.Regex(pattern)
        let schemafun (input:Primitives.Schema) = 
            List.foldBack (fun col (cur:Primitives.Schema) -> cur.Add (Column.Make(col, Primitives.StringElement))) toNames input 
        let regexMapFun (outputSch:Schema) (input:Primitives.Tuple) =             
            let str = input.GetString fromName
            let m:System.Text.RegularExpressions.Match = regex.Match(str)
            let matches = 
                toNames |> 
                List.map 
                    (fun (col:string) ->
                        (match m.Groups.[col] with | null -> "" | _  as grp -> grp.Value) |> Primitives.Datum.String )
            Tuple.Create outputSch (matches @ input.values)                                
        
        FastMapPipe.Create((sprintf "AddColumnsRegex(%A)" pattern), regexMapFun, schemafun)
    
    /// <summary>Adds a single column by filtering an existing string column with a regular expression pattern.
    /// The contents of the new column is either the first match group of the matching string, or the empty string
    /// if there is no such group.</summary>
    /// <example><c>AddColumnRegex ("name","suffix",".*(?\..*)$")</c></example>
    /// <param name="fromName">The name of the column to filter.</param>
    /// <param name="toName"s>The list of names of columns to add.</param>
    /// <param name="pattern">The regular expression pattern.</param>                 
    let AddColumnRegex (fromName,toName,pattern) =
        let regex:System.Text.RegularExpressions.Regex = new System.Text.RegularExpressions.Regex(pattern)
     
        let schemafun (input:Primitives.Schema) = input.Add (Column.Make(toName, Primitives.StringElement))
        let mapfun (input:Primitives.Tuple) = 
           let v = input.GetString fromName
           let m:System.Text.RegularExpressions.Match = regex.Match(v)
           let result = if (m <> null) then  m.Value else ""
           Primitives.Datum.String result
        
        SimpleMapAdd (Column.Make(toName, Primitives.StringElement)) (mapfun) 
    
    /// <summary>Filters a given column. Any row for whom the fiven predicate returns <c>false</c> is dropped.</summary>
    /// <example>FilterColumn "value" (fun datum -> datum <> DatumZero)</example>
    /// <param name="name">The name of the column to filter on.</param>
    /// <param name="p">The predicate, taking a <see cref="Primitives.Datum" />  and returning a <see cref="bool" />.</param>
    let FilterColumn name (p:Primitives.Datum->bool) =
        let f (input:Tuple) = input.GetDatum name |> p 
        SimpleFilter f
                                
    
    /// <summary>Sorts the pipeline based on a composite key.</summary>
    /// <param name="key">The list of column names. The first name indicates the primary simple key, the second one the secondary one,
    /// and so on.</param>
    let SortByKey (key:SchemaKey) =                
        let table = //MetricsDb.MetricsStorageTable.Create ()
                    MetricsStorage.MultiStorage.Create (5,MetricsDb.MetricsStorageTable.Create)
                
        {new IPipe with
            member x.Name () = "Sort"            
            member x.SchemaEffect sch = sch
            member x.Run input f =                
                // if it is already ordered, do nothing
                if key.IsOrdered input.Schema then f input 
                else
                let keyfun (tuple:Tuple) = 
                    key.Fields |> List.map tuple.GetDatum                  
                table.AddSeq keyfun input                                                    
                (table.Read()).AddKey key |>  f                
            }
            
    /// Get a callback for a sliding window of tuples. I.e if there are tuples a,b,c,d and you pass 2 for the window size
    /// you get a callback with [a;b] [b;c] [c;d] [d] 
    let SlidingWindow (count:int) (windowFun:(Schema->Tuple array -> Tuple)) (schemaFun:(Schema -> Schema)) = 
        {new IPipe with
            member x.Name () = "SlidingWindow"            
            member x.SchemaEffect sch = schemaFun sch
            member x.Run input f =

                let sch = schemaFun input.Schema
                let runWindowFun (arr:Tuple array) (ctr:int) length =
                    let initFun ii =  
                        let idx =  (ii + ctr) % arr.Length
                        arr.[idx]
                    let newArr = Array.init length initFun
                    windowFun sch newArr
                
                let tseq =                       
                    seq { 
                        let ctr = ref 0
                        let iter = input.tseq.GetEnumerator ()
                        let isDone =  ref false
                        // pull off the first count elements for the initial window
                        let arr = [| while (!ctr < count) && iter.MoveNext ()  do ctr := !ctr + 1; yield iter.Current |] 
                        // if we have enough elements for a full window, keep going
                        if arr.Length = count then
                            while iter.MoveNext () do                                   
                                yield runWindowFun arr !ctr arr.Length
                                arr.[!ctr % arr.Length] <- iter.Current
                                ctr := (!ctr + 1) % arr.Length
                                
                        // run down the end. We're going to have smaller arrays
                        for ii in 0..(arr.Length-1) do
                            yield runWindowFun arr (!ctr + ii) (arr.Length - ii)
                        }
                TupleSequence.Create sch tseq |> f
            }
            
    /// Adds a field to an outer tuple without changing the inner tuple
    /// it calls a function to select a value for the new field and adds it.
    /// For example {foo=2, qux={bar='a';baz='z'}{bar='a';baz='x'}} would become
    ///            {foo=2, added='3', qux={bar='a';baz='z'}{bar='a';baz='x'}}
    /// when called with "added" (fun f -> String "3") 
    let AddOuterColumn (col:Column) tupName (cgen) =
        let mapfun (tup:Primitives.Tuple) =
            let innerTupSeq = tup.GetTuples tupName
            cgen innerTupSeq                                         
            
        MapAddPipe.Create(sprintf "AddOuterColumn(%A->%A)" col.name tupName,mapfun,(fun _ -> col))
      
    /// Flatten an inner sequence named by a field. This is the reverse of a group operation. The
    /// named field, which should be a tuple sequence, will be taken and appended to the outer fields in     
    /// addition to all of the inner fields.
    let Flatten fieldName =                                
        {new IPipe with
            member x.Name () = sprintf "Flatten(%A)" fieldName
            member x.SchemaEffect sch = 
                let innerSch = sch.InnerSchema fieldName                 
                let outerSch  =(sch.DropColumns [fieldName]).Concat innerSch
                let newKey = 
                    if innerSch.Key.IsSingleton () then
                        outerSch.Key
                    else if outerSch.Key.isUnique then
                        SchemaKey.Create (outerSch.Key.orderings @ innerSch.Key.orderings)                        
                    else
                        outerSch.Key.SetUnique false                                                              
                outerSch.SetKey newKey
                    
            member x.Run input f =
            
                let sch = x.SchemaEffect input.Schema
                let limitedSch = input.Schema.DropColumns [fieldName]
                let nseq = seq { 
                    for tup in input.tseq do
                        let tuples = tup.GetTuples fieldName
                        let limited = tup.FillSchema limitedSch
                        for innerTuple in tuples.tseq do
                            yield Tuple.ConcatToSchema sch [limited;innerTuple] } |> input.NewSeq sch                    
                nseq |> f            
            }

    /// Updates a single field in place. Note this should not
    /// change the schema.
    let UpdateColumn columnName (updateFun:Tuple -> Datum -> Datum) =
        let updateFun   (tup:Primitives.Tuple) =
            let datum = tup.GetDatum columnName 
            tup.UpdateValue columnName (updateFun tup datum)        
        let orderFun ord = reorderSome (fun field -> field = columnName) ord
        let schemaFun (sch:Schema) = sch
        MapPipe.Create  (sprintf "UpdateColumn(%A)" columnName,updateFun,orderFun,schemaFun)

    /// Updates a field that is a tuple sequence in place
    let UpdateTuplesColumn 
        (colName,innerSchemaFun:Schema -> Schema)
        (tuplesUpdateFun:Primitives.Tuple -> Primitives.TupleSequence -> Primitives.TupleSequence ) 
        =       
        let updateTuplesFun (finalSchema:Schema) (tup:Tuple) =
            let tseq = tup.GetTuples colName |> tuplesUpdateFun tup                          
            let newEl = tseq.Schema |> SequenceElement
            tup.UpdateValuesWithSchema finalSchema [colName,Primitives.Datum.Tuples tseq]            
                
        let schemaFun (sch:Schema) = 
            let colSchema = innerSchemaFun (sch.InnerSchema colName)
            sch.Update [colName, Column.Make(colName, colSchema |> SequenceElement)]
        FastMapPipe.Create  (sprintf "UpdateTuplesColumn(%A)" colName,updateTuplesFun,schemaFun)
            
    /// Update an int-typed column in place.
    let UpdateIntColumn columnName (intUpdateFun:Tuple->int64 -> int64) =
        let columnUpdateFun tup (datum:Primitives.Datum) =
            Primitives.Datum.ExtractInt datum |>
            intUpdateFun tup |>
            Primitives.Datum.Int
        UpdateColumn columnName columnUpdateFun
    
    /// Update an string-typed column in place.    
    let UpdateStringColumn columnName (stringUpdateFun:Tuple->string-> string) =
        let columnUpdateFun tup (datum:Primitives.Datum) =
            Primitives.Datum.ExtractString datum |>
            stringUpdateFun tup |>
            Primitives.Datum.String
        UpdateColumn columnName columnUpdateFun
        
    /// Add column to an inner schema.
    let AddInnerColumns colName (schema:Schema) (mapfun:Tuple->Tuple->Tuple) =
            
        let innerSchemaFun (sch:Schema) = schema.Concat sch            
        let outerMapFun (schemaResult:Schema) (outerTup:Tuple) =            
            let innerTupSeq = outerTup.GetTuples colName
            let innerMapFun (sch:Schema) (tup:Tuple) = 
                    let tup' = mapfun outerTup tup
                    Tuple.ConcatToSchema sch [tup';tup]
            let innerMapPipe = FastMapPipe.Create ("Inner Map Add", innerMapFun,innerSchemaFun)
            let innerSeq = RunPipeFromSeq innerTupSeq innerMapPipe
            
            outerTup.UpdateValuesWithSchema schemaResult [colName, innerSeq |> Datum.Tuples]
                        
        let outerSchemaFun (sch:Schema) = 
            
            let colSchema = innerSchemaFun (sch.InnerSchema colName)
            sch.Update [colName, Column.Make(colName, colSchema |> SequenceElement)]
        FastMapPipe.Create  (sprintf "AddInnerColumns(%A)" colName,outerMapFun,outerSchemaFun)
                
    /// <summary>Adds a column. The actual values for the column
    /// are generated by the given function through a pipe.</summary>
    /// <param name="col">The name of the column.</param>
    /// <param name="typ">The <see cref="SchemaElement" /> describing the type of the column.</param>
    /// <param name="cgen">The function generating values for the new column. For each row of the pipeline
    /// the function is called with that row <see cref="Tuple" /> and generates a <see cref="Datum" />
    /// value.    let AddColumn (col:Column) cgen =
    let AddColumn (col:Column) cgen =
        let mapfun (tup:Primitives.Tuple) = cgen tup        
        MapAddPipe.Create (sprintf "AddColumn(%A)" col.name,mapfun,(fun _ -> col))
    
    /// Adds columns with a given schema, generated by the given function through a pipe whioch will be named pipeName. 
    let AddColumns pipeName (sch:Schema) cgen =
        let schemafun (schema:Schema) = schema.Concat sch
        let mapfun (sch:Schema) (tup:Primitives.Tuple) =
            Tuple.ConcatToSchema sch [tup;cgen tup]                   
        FastMapPipe.Create (sprintf "AddColumns(%A)" pipeName,mapfun,schemafun)
        
    /// Fills in missing columns to get the resulting schema *)
    let FillColumns (sch:Schema) =
        let schemafun (schema:Schema) = sch
        let mapfun (sch:Schema) (tup:Primitives.Tuple) =
            let data = 
                [for (outerComp:Column) in sch.Columns do
                    if tup.schema.ContainsColumn outerComp.name then
                        let dtm = tup.GetDatum outerComp.name
                        yield dtm.Coerce ((tup.schema.GetColumn outerComp.name).element,outerComp.element)
                    else
                        yield outerComp.def]
                
            Tuple.Create sch data
        FastMapPipe.Create (sprintf "FillColumns {%A}" sch,mapfun,schemafun)
    
    /// Renames columns, specifying a function to do the name mapping.
    let RenameColumnsBy pipeName renFun =
        let updatefun (comp:Column) = (comp.name, comp.SetName (renFun comp.name))
        let schemafun (sch:Schema) = sch.Columns |> List.map updatefun |> sch.Update
        let mapfun (newSch:Schema) (tup:Tuple) = Tuple.Create newSch tup.values        
        FastMapPipe.Create (sprintf "RenameColumnsBy",mapfun,schemafun) 
        
    /// Renames columns, specifying a list of (before,after) pairs. Anything not mentioned explicitly
    /// will not be renamed.
    let RenameColumns (renamings:(string * string) list) =
        let renameMap = Map.ofList renamings
        let renFun field = match Map.tryFind field renameMap with
                           | None -> field
                           | Some replace -> replace      
        let updatefun (comp:Column) = comp.name , comp.SetName (renFun comp.name)      
        let schemafun =
            let save = ref None
            fun (sch:Schema) ->
                if !save = None then 
                    save := sch.Columns |> List.map updatefun |> sch.Update |> Some
                (!save).Value
        
        let mapfun newSch (tup:Tuple) = Tuple.Create newSch tup.values                
        FastMapPipe.Create (sprintf "RenameColumns(%A)" renamings,mapfun,schemafun)         
        
    /// Renames columns inside an inner tuple sequence. That is, given a field
    /// that is a tuple sequence, rename columns in that inner sequence.
    let RenameInnerColumns (outerField:string) (renamings:(string * string) list) =
        let renameMap = Map.ofList renamings
        let renFun field = match Map.tryFind field renameMap with
                           | None -> field
                           | Some replace -> replace      
        let updatefun (comp:Column) = comp.name , comp.SetName(renFun comp.name)
        
        let innerSchemaFun (sch:Schema) = sch.Columns  |> List.map updatefun |> sch.Update
        let schemaFun = 
            let save = ref None
            fun (sch:Schema)  ->
            if !save = None then
                let innerSch = (innerSchemaFun (sch.InnerSchema outerField)) |> SequenceElement
                save := sch.Update [outerField,Column.Make(outerField,innerSch)] |> Some
            (!save).Value
        
        let renameInnerMapFun (resultSchema:Schema) (tup:Tuple) = 
            let inner = tup.GetTuples outerField
            let innerSch = resultSchema.InnerSchema outerField            
            let tseq = inner.tseq |> Seq.map (fun (tup:Tuple) -> Tuple.Create innerSch tup.values)
            let tupseq = inner.NewSeq innerSch tseq |> Datum.Tuples
            let index = tup.schema.NameIndex outerField
            let values = tup.values  |> List.mapi (fun ii v -> if ii = index  then tupseq else v) 
            Tuple.Create resultSchema values
                                
        FastMapPipe.Create (sprintf "RenameInnerColumns(%A)" renamings,renameInnerMapFun,schemaFun)
        
    /// Function pipe that saves the maximum value encountered in a column
    let ColumnMax col =
        let initial = System.Int64.MinValue |> Primitives.Datum.Int
        let func (tup:Primitives.Tuple) (max:Primitives.Datum) =                                             
                let field = tup.GetDatum col
                if (field > max) then field else max
        Pipes.FunctionPipe.Create (initial,func)
    
    /// Assigns unique ids to a column. Anything that is 0 will get a new id.
    let AssignUniqueIds col =
        let table = MetricsDb.MetricsStorageTable.Create ()
        let nth = ref 0L
        
        {new IPipe with
            member x.Name () = sprintf "AssignUniqueIds(%A)" col            
            member o.SchemaEffect sch = sch
            member x.Run  (input:TupleSequence) f=                
                let max = ColumnMax col                        
                table.AddSeq (fun tup -> nth := !nth + 1L; [!nth |> Primitives.Datum.Int]) input
                // iterate the saved table over the max pipe
                let simpleSink = (fun tseq -> for _ in tseq do () )
                (max :> IPipe).Run (table.Read ()) simpleSink            
                let current = (max.Result () |> Primitives.Datum.ExtractInt) |> ref
                let key = SchemaKey.Create([Ascending,col],true)
                let schema = table.ReadSchema().SetKey key
                let zero = Datum.IntZero
                let tseq =
                    seq {                        
                        for tup:Primitives.Tuple in (table.Read ()) do
                            let existing = tup.GetDatum col
                            if (existing = zero) then 
                                current := !current + 1L
                                yield tup.UpdateValue col (!current |> Primitives.Datum.Int)
                            else 
                                yield tup
                    }                    
                tseq  |> TupleSequence.CreateKeyed schema |> f
        } 
 
    let private printLock = new obj ()
    
    /// Global variable that allows you to redirect the print rows to an alternate output stream.
    /// The default is stdout.
    let mutable public PrintRowsWriter:System.IO.TextWriter = System.Console.Out
    
    let private printOne total (name,rowFun,isDoneFun,lastFun)  = 
        let rows = rowFun ()
        let isDone = isDoneFun ()
        let lastVal = lastFun rows        
        let delta = rows - lastVal
        fprintfn PrintRowsWriter "%s:\t%d\t(+%d)%s" name rows delta (if isDone then "*" else "")
        total + delta
        
    let private printRowsUpdater =
        let printRowList = new System.Collections.Generic.List<string * (unit -> int) * (unit -> bool) * (int -> int)> ()
        let killThread = new System.Threading.ManualResetEvent false        
        let threadFun () = 
            System.Threading.Thread.CurrentThread.Name <- "StatusPrinter" 
            let initialTime = System.DateTime.Now
            
            while killThread.WaitOne(30 * 1000) = false do
                let printAndUpdate _ =
                    let currentTime = System.DateTime.Now
                    let elapsed = currentTime.Subtract initialTime
                    fprintfn PrintRowsWriter "Updated at %s (%s elapsed)" (currentTime.ToString()) (elapsed.ToString())
                    
                    let total = printRowList |> Seq.fold printOne 0
                    if total = 0 then 
                        fprintfn PrintRowsWriter "****** No progress in update interval ********"
                    fprintfn PrintRowsWriter ""
                    PrintRowsWriter.Flush ()
                    ()
                lock printLock printAndUpdate  
            killThread.Reset () |> ignore

        let printThread = new System.Threading.Thread (threadFun)
        let addFun (n,fcheck,fdone) =
            safeStart printThread
            let lastVal = ref 0
            let getLast cur =                                 
                let last = !lastVal
                lastVal := cur
                last
                
            lock printLock (fun _ -> printRowList.Add (n,fcheck,fdone,getLast))
        let trimFun () = 
            lock printLock 
                (fun _ -> 
                    let undoneRows = printRowList |> Seq.filter (fun (_,_,doneFun,_) -> doneFun () = false) |> List.ofSeq
                    if List.length undoneRows = 0 then 
                        killThread.Set () |> ignore
                        printThread.Join ()
                    printRowList.Clear ()
                    printRowList.AddRange undoneRows
                )
        
        addFun,trimFun
        
    /// Print name and status of the pipe to a log periodically. Also will print when the 
    /// pipe has completed.
    let PrintStatus name =             
        {new IPipe with
            member x.Name () = sprintf "PrintRows(%s)" name            
            member o.SchemaEffect sch = sch        
            member x.Run input f =                    
                let printStack = false
                let idx = ref 0
                let finished = ref false
                let initialTime = System.DateTime.Now        
                let startTime = ref System.DateTime.Now        
                let printSchema = false
                let addFun,trimFun = printRowsUpdater         
                if !idx = 0 then addFun (name,(fun () -> !idx),(fun () -> !finished))
                let tseq =                                   
                    seq {                            
                        let tid = System.Threading.Thread.CurrentThread.ManagedThreadId                                                        
                        if !idx = 0 then                                
                            startTime := System.DateTime.Now
                            lock printLock 
                              (fun () ->                                             
                                    if printStack then fprintfn PrintRowsWriter "[%d]:%s: beginning %A" tid name (new System.Diagnostics.StackTrace ())
                                    else fprintfn PrintRowsWriter "[%d]:%s: beginning" tid name
                                    if printSchema then fprintfn PrintRowsWriter "[%d]:%s: schema %s" tid name (input.Schema.ToString ())                                            
                               )
                        
                        for tup in input.tseq do                        
                            let inc = System.Threading.Interlocked.Increment idx                                                            
                            yield tup                                
                        finished := true
                        let cnt = !idx
                        let currentTime = System.DateTime.Now
                        let elapsed = currentTime.Subtract !startTime
                        let elapsedTotal = currentTime.Subtract initialTime
                        let tid = System.Threading.Thread.CurrentThread.ManagedThreadId
                        lock printLock (fun () -> fprintfn PrintRowsWriter "[%d]:%s: complete %d (%s/%s)" tid name cnt (elapsed.ToString()) (elapsedTotal.ToString()))
                        PrintRowsWriter.Flush ()
                        trimFun ()                
                    }
                input.NewSeq input.Schema  tseq |> f                
        } 
            
        
    /// Take two tuple sequences with the same schema, and merge the two such that the newSeq preempts the oldSeq when the keys
    /// match
    let JoinCollateOrderedSequences keyFields oldSeq newSeq =
        let orderedMerge = Join.JoinCollateOrdered keyFields
        let collector = new CollectorSink ()        
        let cont = (collector :> ISink).Sink
        orderedMerge.Join newSeq oldSeq cont
        collector.GetSeq        
        
    /// Limits a pipe to a certain number of tuples. 
    /// Takes a pair of ints -- the first is how many tuples to skip,
    /// then the next is how many allow before stopping.
    let Limit (limiters:(int64 * int64)) =        
        let seenRef = ref 0L        
        let filterFun _ =
            let seen = !seenRef
            seenRef := seen + 1L
            let skip,limit = limiters
            let max = skip + limit
            if seen < skip then FilterDrop else
            if seen >= max then FilterDone else
            FilterAccept
        FilterPipe.Create("Limiter",filterFun)
    
    /// take only a subsample of tuples, given
    /// a key (int, or unsigned field) 
    /// and a sample rate M and sample offset N.
    /// The tuples that pass are of the form
    /// tup[key] mod M = N
    let Downsample name (limiters:Lazy<Datum * Datum>) =                
        let filterFun (tup:Tuple) =            
            let rate,offset = limiters.Value
            let key = tup.GetDatum name
            let accept = 
                match (tup.schema.GetColumn name).element with 
                | IntElement ->
                    let ratei = Datum.ExtractInt rate
                    let offseti = Datum.ExtractInt offset
                    let keyi = Datum.ExtractInt key
                    keyi % ratei  = offseti
                | UnsignedElement ->
                    let rateu = Datum.ExtractUnsigned rate
                    let offsetu = Datum.ExtractUnsigned offset
                    let keyu = Datum.ExtractUnsigned key
                    keyu % rateu = offsetu
                | _ -> failwith "unsupported type"
            if accept then FilterAccept else FilterDrop
        FilterPipe.Create("Downsample",filterFun)
        
    /// <summary>Assumes a specific ordering in the pipeline. This does not verify the ordering.</summary>
    /// <param name="key">The key specifying the ordering.</param>
    let AssumeOrdering (key:SchemaKey) =
        MapPipe.Create(sprintf "AssumeOrdering(%A)" (key.ToString()), (fun x->x), (fun _->key), schemaPreserving)
    
    /// <summary>Verifies that the pipeline is ordered by a given key.</summary>
    /// <param name="key">The key specifying the ordering.</param>
    let rec ExpectOrdering (key:SchemaKey) =
       {new IPipe with
            member x.Name() = sprintf "ExpectOrdering(%A)" (key.ToString())
            member x.SchemaEffect sch = sch
            member x.Run input f =
                    
                let rec checkOrd (old:Tuple) (cur:Tuple) order =
                    match order with
                    | [] -> ()
                    | (Ascending, f) :: tl -> 
                        let tval = cur.GetDatum f
                        let lval = old.GetDatum f
                        let diff = (tval :> System.IComparable).CompareTo lval
                        if diff < 0 then                             
                            raise <| Primitives.OrderingValidation 
                                        (sprintf "Order violation for (Ascending, %A) in %A: expected %A >= %A" f order tval lval)
                        else if  key.isUnique && diff = 0 && List.isEmpty tl then 
                            raise <| Primitives.OrderingValidation 
                                        (sprintf "Exclusive Order violation for (Ascending, %A) in %A: expected %A > %A" f order tval lval)
                        else if tval = lval then
                            checkOrd old cur tl                                                    
                    | (Descending, f) :: tl -> 
                        let tval = cur.GetDatum(f)
                        let lval = old.GetDatum(f)                        
                        if tval > lval then 
                            raise <| Primitives.OrderingValidation 
                                        (sprintf "Order violation for (Descending, %A) in %A: expected %A <= %A" f order tval lval)
                        else if key.isUnique && tval = lval && List.isEmpty tl then 
                            raise <| Primitives.OrderingValidation 
                                        (sprintf "Exclusive Order violation for (Ascending, %A) in %A: expected %A < %A" f order tval lval)
                        else if tval = lval then
                            checkOrd old cur tl
                         
                    | _ :: tl -> checkOrd old cur tl     
                    
                
                let tseq =                                   
                    seq {
                        let last = ref None          
                        for tup in input.tseq do                                
                            match !last with
                              | Some t -> checkOrd t tup key.orderings
                              | None -> ()
                            last := Some tup
                            yield tup
                    }
                let newSchema = input.Schema.SetKey key
                TupleSequence.CreateKeyed newSchema tseq |> f   // The new sequence has the order that we've validated.
            }  
        
    /// Allow you to insert a 'probe' that will call you back when a certain condition is true.
    /// Generally used with things that cause side effects
    let Probe (probePredicate:Tuple->bool) (probe:Tuple->unit) =
        let mapfun (tup:Tuple) = 
            if (probePredicate tup) then probe tup
            tup                    
        MapPipe.Create("Probe",mapfun,orderPreserving,schemaPreserving)
                
    
    /// Inserts a probe that will print the value of any tuple that matches the probe predicate
    let PrintProbe messageFun probePredicate =
        let probeFun tup = fprintfn PrintRowsWriter "%s" (messageFun tup); ()
        Probe probePredicate probeFun
    
    /// Inserts a probe that will print the schema at that point. Is useful for debugging.
    let PrintSchemaProbe name =
        let shouldPrint = ref true
        let predicate (tup:Tuple) =  
            let result = !shouldPrint
            shouldPrint := false
            result
        let messageFun (tup:Tuple) = sprintf "%s:%s" name (tup.schema.ToString())
        PrintProbe messageFun predicate
            
            
    
    /// Exception that gets fired when an AssertProbe predicate is violated.
    exception ProbeAssertion of string * Tuple  
    /// Allows you to insert a probe that asserts that something is true about a tuple
    let AssertProbe probePredicate message =
        let probeFun tup =
            raise <| ProbeAssertion (message, tup)
        Probe (fun t -> probePredicate t |> not) probeFun
        
    /// Allows you to run a tuple sequence datum through a pipe. Note because of setup and teardown
    /// costs, this can be a performance bottleneck.
    let InnerPipe (inFieldName,outSchemaGen) (pipes:IPipe) =
        let mapAddFun (tuple:Primitives.Tuple) =  
            let inSeq = tuple.GetTuples inFieldName
            RunPipeFromSeq inSeq pipes |> Datum.Tuples
                    
        MapAddPipe.Create(sprintf "Inner(%s)" inFieldName,mapAddFun,outSchemaGen)               
    
    /// <summary>Sorts a tuples column by based on a composite key of its inner columns.</summary>
    /// <param name="name">The name of the column to sort.</param>
    /// <param name="innerNames">The list of inner column names. The first name indicates the primary simple key, the second one the secondary one,
    /// and so on.</param>
    let SortTuplesColumnByInnerColumns name innerNames =
        let innerSchFun =
            let saved = ref None
            fun (sch:Schema) -> 
                if !saved = None then 
                    saved := innerNames |> List.map (fun fld -> Ascending,fld) |> sch.Key.SetOrderings |> sch.SetKey |> Some
                (!saved).Value
                
        let updateFun tup (tseq:TupleSequence) =
            let lst = tseq.tseq |> List.ofSeq
            if lst.IsEmpty then  TupleSequence.Create (innerSchFun tseq.Schema) Seq.empty else
            let newTseq = lst |> List.sortBy (fun (tup:Tuple) -> innerNames |> List.map (fun fld -> tup.GetDatum fld)) |> Seq.ofList
            TupleSequence.CreateKeyed (innerSchFun tseq.Schema) newTseq
        
        UpdateTuplesColumn (name,innerSchFun) updateFun
