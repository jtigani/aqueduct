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
#nowarn "45" // skip "This method will be made public in the underlying IL because it may implement an interface or override a method"
namespace Aqueduct


/// This module implements pipe joins 
module Join =
    open Aqueduct
    open Aqueduct.Primitives
    open Aqueduct.Pipes
    
    module Internal = 

        type tupIter = System.Collections.Generic.IEnumerator<Tuple>
        type JoinState = LeftDone | RightDone | BothDone | LimitReached 
        
        type AdvanceJoin = AdvanceNext | AdvanceNone
        
        (* Specialized pipe that performs a join between two tuples. *)
        type internal IJoin =
            (* Key generation function for the left side of the join (the current pipe stage) *)
            abstract KeyGen: JoinSide -> Tuple -> Datum list
            (* Indicates how this combines a left and right schema *)        
            abstract SchemaEffect: Schema -> Schema -> Schema
            (* Decides how to join two tuples that ar the left and right halves of the join
               schema is the expected resulting schema *)
            abstract JoinTuples: Schema->Tuple -> Tuple -> Tuple
            (* When doing an outer join, decide what to return when there is a match
               on one side but not the other.
               Args are side to join on (left vs right) the expected schema
               of the result, and the tuple of the side that didn't miss the join.
               The result is a new tule with the passed in schema. *)
            abstract Fill: JoinSide -> Schema -> Tuple -> Tuple 
            (* Describes the naturla sort order of the keys*)           
            abstract SortKeys: JoinSide->string list
        
        type internal IOrderedJoin =
            abstract Name: unit->string
            abstract SchemaEffect: Schema->Schema->Schema
            abstract SortKeys: JoinSide->string list
            abstract KeyEffect: SchemaKey->SchemaKey->SchemaKey
            abstract Process: Schema->Tuple -> Tuple ->AdvanceJoin * AdvanceJoin * Tuple option
            abstract LeftRemainder: Schema->Tuple ->AdvanceJoin * Tuple option
            abstract RightRemainder: Schema->Tuple ->AdvanceJoin * Tuple option
                
            (* Applies an inner join. This can be thought of as filtering this pipe by the contents of another pipe.
           this should be used when the magnitude of this pipe is much large than the magnitude of the other pipe (
           the other pipe being the tap argument). For this to work, the other tap should fit comfortably in memory.
           If the other doesn't fit in memory, a different join should be used.
           Future work will automatically apply the right kind of join. That isn't implemented currently.
         *)
         
         type internal SmallJoinInfo =
            {saveRight:System.Collections.Generic.Dictionary<Datum list,Tuple>
             seenRight:System.Collections.Generic.Dictionary<Datum list,bool>
             mutable leftSchema:Schema option
             mutable rightSchema:Schema option
             }
            with
            static member Create() =
                {saveRight=new System.Collections.Generic.Dictionary<Datum list,Tuple>()
                 seenRight=new System.Collections.Generic.Dictionary<Datum list,bool>()
                 leftSchema=None
                 rightSchema=None}
        
        let internal SmallJoin (ijoin:IJoin,joinDirection:JoinDirection)  =        
            let merge =
                {new IBlockingMerge<SmallJoinInfo> with
                    member x.Name () = sprintf "Small%AJoin"  joinDirection
                    member x.SchemaEffect left (right:Schema) = 
                        let combinedSch = ijoin.SchemaEffect left right
                        let leftKey = left.Key
                        let newKey = 
                            match joinDirection,leftKey.isUnique,right.Key.isUnique with
                            | JoinInner,_,_ -> leftKey
                            | JoinLeftOuter,true,false -> leftKey.SetUnique false
                            | JoinLeftOuter,_,_ -> leftKey
                            | JoinFullOuter,true,true -> SchemaKey.Empty.SetUnique true 
                            | JoinFullOuter,_,_ -> SchemaKey.Empty // for full outer join, we don't know if we'll have
                                                      // to add values from the right side, so we don't know
                                                      // what the final ordering will be.
                        combinedSch.SetKey newKey
                        
                    member x.InitialRightValue () = SmallJoinInfo.Create ()                
                    member x.CollectRight tseq joinInfo =                    
                        if joinInfo.rightSchema = None then joinInfo.rightSchema <- Some tseq.Schema
                        for (tup:Tuple) in tseq.tseq do
                            joinInfo.saveRight.[ijoin.KeyGen RightSide tup] <- tup
                        joinInfo
                    member x.RunLeft (joinInfo:SmallJoinInfo) aseq f =
                        let getSchema () = 
                            (x :> IBlockingMerge<_>).SchemaEffect joinInfo.leftSchema.Value joinInfo.rightSchema.Value
                        let createTseq (schema:Schema) tseq  =                        
                            TupleSequence.CreateKeyed schema tseq
                        if joinInfo.leftSchema = None then joinInfo.leftSchema <- Some aseq.Schema    
                        let keygen tup = ijoin.KeyGen LeftSide tup
                        let innerJoinFilter tup = joinInfo.saveRight.ContainsKey (keygen tup)                                            
                        let resultSchema = getSchema ()
                        let combineWithSaved tup = 
                            let key = keygen tup
                            ijoin.JoinTuples resultSchema tup (joinInfo.saveRight.[key]) 
                        let fillRight tup = ijoin.Fill RightSide resultSchema tup
                        let leftOuterJoin (tup:Tuple) =
                            let key = keygen tup
                            if innerJoinFilter tup then 
                                joinInfo.seenRight.[key] <- true
                                combineWithSaved tup
                            else fillRight tup
                            
                        let newseq =                             
                            match joinDirection with
                            | JoinInner -> aseq |> Seq.filter innerJoinFilter 
                                           |> Seq.map combineWithSaved
                            | JoinLeftOuter
                            | JoinFullOuter ->
                                // at this point a full outer and a left outer
                                // join look the same.
                                aseq.tseq |> Seq.map leftOuterJoin
                        
                        let remainderSeq = 
                            // for full outer join, we need to add
                            // a remainder from the right side
                            match joinDirection with
                            | JoinFullOuter ->
                                let fillLeft tuple = ijoin.Fill LeftSide resultSchema tuple 
                                seq {
                                    for key in joinInfo.saveRight.Keys do
                                        if not <| joinInfo.seenRight.ContainsKey key then 
                                            yield fillLeft (joinInfo.saveRight.[key])
                                }                            
                            | _ -> Seq.empty
                            
                        let tupseq = createTseq resultSchema (Seq.append newseq remainderSeq)
                        tupseq |> f
                }
            SequentialMergePipe.Create(merge)
                        
        let rec private keyComp (ord:Ordering list) (a:Datum list) (b:Datum list) = 
                match a, b,ord with
                | [],[],_ -> 0            
                | l :: ltl, r :: rtl, (Ascending,_)::ordtl -> 
                    let diff = (l :> System.IComparable).CompareTo r in if diff <> 0 then diff else keyComp ordtl ltl rtl 
                | l :: ltl, r :: rtl, (Descending,_)::ordtl -> 
                    let diff = (l :> System.IComparable).CompareTo r in if diff <> 0 then -diff else keyComp ordtl ltl rtl            
                | _ -> failwith "unexpected key mismatch"
                
        type internal OrderedJoinPipe = 
            val private orderedJoin: IOrderedJoin
            
        with    
            static member internal CreateInner(join) = new OrderedJoinPipe (OrderedJoinPipe.orderedPipeInner join) :> IJoinPipe
            static member internal CreateLeftOuter(join) = new OrderedJoinPipe (OrderedJoinPipe.orderedPipeLeftOuter join) :> IJoinPipe
            static member internal CreateFullOuter(join) = new OrderedJoinPipe (OrderedJoinPipe.orderedPipeFullOuter join) :> IJoinPipe
                    
            private new(ord:IOrderedJoin) = {orderedJoin=ord}
                                            
            static member private orderedPipeInner (joiner:IJoin) = 
                {new IOrderedJoin with 
                    member x.Name () = "Inner"
                    member x.SortKeys side = joiner.SortKeys side
                    member x.SchemaEffect lsch rsch = joiner.SchemaEffect lsch rsch
                    member x.KeyEffect lkey rkey = lkey
                                        
                    member x.Process resultSch left right =                    
                        let lkey = joiner.KeyGen LeftSide left
                        let rkey = joiner.KeyGen RightSide right                    
                        match keyComp resultSch.Ordering lkey rkey with                                        
                        | diff when diff = 0 -> AdvanceNext, AdvanceNone, (joiner.JoinTuples resultSch left right|> Some)
                                                
                        | diff when diff < 0 -> AdvanceNext,AdvanceNone,None                        
                        | diff when diff > 0 -> AdvanceNone,AdvanceNext,None                    
                        | _ -> failwith "impossible condition"
                    member x.LeftRemainder _ leftTup = AdvanceNext,None
                    member x.RightRemainder _ rightTup = AdvanceNext,None                    
                }
                
            static member private orderedPipeLeftOuter (joiner:IJoin)  = 
                {new IOrderedJoin with 
                    member x.Name () = "LeftOuter"
                    member x.SchemaEffect lsch rsch = joiner.SchemaEffect lsch rsch
                    member x.KeyEffect lkey rkey = 
                        if rkey.isUnique = true then lkey
                        else lkey.SetUnique false
                        
                    member x.SortKeys side = joiner.SortKeys side
                    member x.Process resultSchema left right =
                        let lkey = joiner.KeyGen LeftSide left
                        let rkey = joiner.KeyGen RightSide right
                        match keyComp resultSchema.Ordering lkey rkey with                    
                        | diff when diff = 0 -> let cur = joiner.JoinTuples resultSchema left right
                                                AdvanceNext,AdvanceNone, Some cur // remember not to advance right-- otherwise multiple matches will miss.
                        | diff when diff < 0 -> let cur =  joiner.Fill RightSide resultSchema left
                                                AdvanceNext,AdvanceNone, Some cur
                        | diff when diff > 0 -> AdvanceNone,AdvanceNext, None
                        | _ -> failwith "impossible condition"
                    member x.LeftRemainder sch left = AdvanceNext,Some (joiner.Fill RightSide sch left)
                    member x.RightRemainder _ right = AdvanceNext,None
                }
            static member private orderedPipeFullOuter (joiner:IJoin)  = 
                {new IOrderedJoin with 
                    member x.Name () = "FullOuter"
                    member x.SchemaEffect lsch rsch = joiner.SchemaEffect lsch rsch
                    member x.KeyEffect lkey rkey = 
                        let keyfields = joiner.SortKeys LeftSide
                        let key' = lkey.Subset keyfields                    
                        if rkey.isUnique = false then key'.SetUnique false
                        else key'
                    member x.SortKeys side = joiner.SortKeys side
                    member x.Process resultSchema left right =
                        let lkey = joiner.KeyGen LeftSide left
                        let rkey = joiner.KeyGen RightSide right                      
                        match keyComp resultSchema.Ordering lkey rkey with                    
                        | diff when diff = 0 -> let cur = joiner.JoinTuples resultSchema left right
                                                AdvanceNext,AdvanceNext, Some cur 
                        | diff when diff < 0 -> let cur =  joiner.Fill RightSide resultSchema left
                                                AdvanceNext,AdvanceNone, Some cur
                        | diff when diff > 0 -> let cur =  joiner.Fill LeftSide resultSchema right
                                                AdvanceNone,AdvanceNext, Some cur
                        | _ -> failwith "impossible condition"
                    member x.LeftRemainder sch left = AdvanceNext,Some (joiner.Fill RightSide sch left)
                    member x.RightRemainder sch right = AdvanceNext,Some (joiner.Fill LeftSide sch right)
                }
            
            member private self.RunMergeRemainder (expectedSchema:Schema) side (iter:tupIter)  =
                let limitLength = 0x400
                let acc = Array.create limitLength (Tuple.Create Schema.Empty [])
                let rec loop count = 
                    let advance,resultOpt =
                        match side with
                        | LeftSide -> self.orderedJoin.LeftRemainder expectedSchema iter.Current
                        | RightSide -> self.orderedJoin.RightRemainder expectedSchema iter.Current
                        | _ -> failwith "unexpected"
                    let ok = if advance = AdvanceNext then iter.MoveNext () else false
                    let count' = 
                        match resultOpt with
                        | Some tup -> acc.[count] <- tup; count + 1
                        | None -> count
                        
                    match ok with
                    | true when count' >= limitLength -> LimitReached,count'
                    | true -> loop(count')                    
                    | false -> BothDone,count'                
                let resultSide,count = loop 0 in (resultSide, Array.sub acc 0 count)
                
            
            member private self.RunMergeJoin (expectedSchema:Schema) (leftIter:tupIter) (rightIter:tupIter) =
                let limitLength = 0x400                          
                let advanceIter adv (iter:tupIter) = if adv = AdvanceNext then iter.MoveNext () else true            
                let acc = Array.create limitLength (Tuple.Create Schema.Empty [])                                        
                let rec loop (nSeen,nAcc) =
                    let advanceLeft,advanceRight,resultOpt = self.orderedJoin.Process expectedSchema leftIter.Current rightIter.Current
                    let leftOk = advanceIter advanceLeft leftIter
                    let rightOk = advanceIter advanceRight rightIter
                    
                    let nAcc'= 
                        match resultOpt with
                        | Some tup -> acc.[nAcc] <- tup; nAcc + 1
                        | None -> nAcc
                        
                    match leftOk,rightOk with
                    | true,true when nSeen + 1 >= limitLength -> LimitReached,nAcc'
                    | true,true -> loop (nSeen + 1,nAcc')
                    | false,true -> LeftDone, nAcc'
                    | true,false -> RightDone, nAcc'
                    | false,false -> BothDone, nAcc'
                let resultSide,count = loop (0,0) in (resultSide, Array.sub acc 0 count)
                
            member self.SchemaEffect left right = 
                    let endSchema = self.orderedJoin.SchemaEffect left right
                    let endKey = self.orderedJoin.KeyEffect left.Key right.Key
                    endSchema.SetKey endKey
                            
            interface IJoinPipe with                        
                member self.Name () = sprintf "OrderedJoin.%s" (self.orderedJoin.Name ())
                member self.Join (leftTseq:TupleSequence) (rightTseq:TupleSequence) (cont:PipeContinuation) : unit = 
                                                                                            
                    // read from the right first, so when the left is ready, we can proceed.                
                    let endSchema = self.SchemaEffect leftTseq.Schema rightTseq.Schema                
                    let expectL = self.orderedJoin.SortKeys LeftSide
                    let expectR = self.orderedJoin.SortKeys RightSide
                    
                    // make sure the ording is acceptable.
                    let checkOrd expect ord side =
                        let ordFields = ord |> List.map snd
                        let list_take n lst = lst |> Seq.ofList |> Seq.take n |> List.ofSeq
                        if (List.length expect > List.length ordFields || expect <> list_take (List.length expect) ordFields) then
                            raise (SchemaValidation 
                            //  (printfn "Warning! %s"
                                (sprintf "ordering violation in ordered merge (%s): expected %A got %A " 
                                            side expect ord))
                        else ()
                        
                    checkOrd expectL leftTseq.Schema.Ordering  "left"
                    checkOrd expectR rightTseq.Schema.Ordering "right"                    

                    let iterL = leftTseq.tseq.GetEnumerator ()
                    let iterR = rightTseq.tseq.GetEnumerator ()
                    let remainderLoop side iter =
                        let state = ref LimitReached
                        seq {
                        while !state = LimitReached do
                            let newState,arr = self.RunMergeRemainder endSchema side iter
                            state := newState                            
                            yield! (arr |> Seq.ofArray)
                        }
                            
                    let innerLoop initialState =
                        seq {
                            let state = ref initialState
                            
                            while !state = LimitReached do 
                                let newState,arr = self.RunMergeJoin endSchema iterL iterR
                                state := newState
                                yield! (arr |> Seq.ofArray)
                            let remainder = 
                                match !state with                             
                                | LeftDone -> remainderLoop RightSide iterR
                                | RightDone-> remainderLoop LeftSide iterL
                                | LimitReached -> failwith "oops"
                                | BothDone -> Seq.empty
                            yield! remainder
                        }
                    let initialState = 
                        match iterL.MoveNext() ,iterR.MoveNext () with
                        | false,false -> BothDone
                        | true,false -> RightDone
                        | false,true -> LeftDone
                        | true,true -> LimitReached
                    let tseq = TupleSequence.CreateKeyed endSchema (innerLoop initialState)
                    tseq |> cont
                                                
        type internal JoinPipe =
            val private joiner:IJoin
            val private joinDirection:JoinDirection        
            val private dbTableR:MetricsStorage.IMetricsStorage
            val private dbTableL:MetricsStorage.IMetricsStorage
        with    
                    
            static member internal Create(join,joinDirection) = new JoinPipe(join,joinDirection) :> IJoinPipe        
            private new(joiner,joinDirection) = {
                joiner=joiner
                joinDirection=joinDirection            
                dbTableR = createStorage JoinLarge false
                dbTableL = createStorage JoinLarge false
                }
                                    
            member private self.GenerateSeq () =
                let schema = self.SchemaEffect (self.dbTableL.ReadSchema ()) (self.dbTableR.ReadSchema ())                
                let tseq = 
                    match self.joinDirection with
                    | JoinInner ->
                        self.dbTableL.JoinInner self.dbTableR (self.joiner.JoinTuples schema)
                    | JoinLeftOuter ->
                        self.dbTableL.JoinLeftOuter self.dbTableR (self.joiner.JoinTuples schema)
                    | JoinFullOuter ->
                        raise <| MetricsStorage.StorageFailure "full outer join not supported for database joins"
                
                TupleSequence.CreateKeyed schema tseq
            
            member self.Spawn () = JoinPipe.Create(self.joiner,self.joinDirection)                            
            member self.KeyGen tup = self.joiner.KeyGen LeftSide tup
                           
            member private self.AnyName joinSide = sprintf "Join.%A-%A" self.joinDirection joinSide
            
            member self.SchemaEffect left right = 
                    let result = self.joiner.SchemaEffect left right                
                    let joinOrder = self.joiner.SortKeys LeftSide |> List.map (fun fld -> Ascending,fld)
                    result.SetOrdering joinOrder
                    
            interface IJoinPipe with            
                member self.Name () = sprintf  "Join.%A" self.joinDirection
                                
                member self.Join (leftTseq:TupleSequence) (rightTseq:TupleSequence) (cont:PipeContinuation) : unit =
                    let addCommands = 
                        seq [async {self.dbTableL.AddSeq (self.joiner.KeyGen LeftSide) leftTseq}
                             async {self.dbTableR.AddSeq (self.joiner.KeyGen RightSide) rightTseq}]
                    addCommands |> Async.Parallel |> Async.RunSynchronously |> ignore
                    self.GenerateSeq () |> cont                                    
                    self.dbTableL.Close ()
                    self.dbTableR.Close ()
                                   
        // IJoin that implements a standard join on a field.                        
        let internal fieldJoin leftKeyFields rightKeyFields= 
            {new IJoin with
                member x.KeyGen side tup = match side with LeftSide -> List.map tup.GetDatum leftKeyFields
                                                          | RightSide -> List.map tup.GetDatum rightKeyFields
                                                          | _ -> failwith "strange"
                member x.SchemaEffect left right =  left.Concat (right.DropColumns rightKeyFields)
                (* We want to concatenate the two tuples, but we don't want the match column -- if they're
                   named the same thing this will cause an error, and if they're named something different
                   it will still cause goofiness *)
                member x.Fill side fullSch tup =                                 
                    match side with 
                    | LeftSide -> 
                        // if we're filling in the left side, we are adding default values for
                        // the keys, we need -- so take the tap fields
                        // and copy them over to the key fields
                        let lkeys = List.map tup.GetDatum rightKeyFields
                        let rkeys = List.zip leftKeyFields lkeys
                        let fullTuple = tup.FillSchema fullSch                
                        fullTuple.UpdateValues rkeys                    
                    | RightSide -> tup.FillSchema fullSch
                    | _ -> failwith "unexpected join side"
                    
                member x.JoinTuples (schema:Schema) (tl:Tuple) (tr:Tuple) =  Tuple.FillFromSchema schema [tl;tr]
                member x.SortKeys (joinSide:JoinSide)=
                    match joinSide with
                    | LeftSide -> leftKeyFields 
                    | RightSide -> rightKeyFields
                    | AnySide ->  leftKeyFields
            } 
            
        // IJoin that merges two pipes, both with the same schema, allowing
        // a function that decides which tuple should be kept, and alternatively,
        // modifying values of the resulting tuple (but must not change the schema).
        // Also you shouldn't muck with the ordering.
        let internal fieldMergeJoin fields mergeFun = 
            {new IJoin with
                member x.KeyGen side tup = List.map tup.GetDatum fields
                member x.SchemaEffect left right =  
                    if left.SetKey (SchemaKey.Empty) <> right.SetKey (SchemaKey.Empty) then 
                        raise <| SchemaValidation (sprintf "merge join requires identical schemas %A <> %A" left right)
                    else if left.Key <> right.Key then
                        left.SetKey (SchemaKey.LeastRestrictive left.Key right.Key)
                    else                    
                        left
                // Fill gets called when there is a join mismatch from one side to the other (if doing an outer 
                // join. If there is a mismatch -- i.e. one key is on one side but not the other, we want to
                // just return the one is there.
                member x.Fill _ _ tup = tup
                member x.JoinTuples (schema:Schema) (tl:Tuple) (tr:Tuple) = mergeFun schema tl tr
                member x.SortKeys (joinSide:JoinSide)= fields
            }        
        
        let internal ColumnJoin (joinSize:JoinSize) (leftFieldName,rightFieldName,outFieldName) (ijoin:IJoin) =
            let runOne leftSeq rightSeq = 
                let joiner = 
                    match joinSize with
                    | JoinOrdered -> OrderedJoinPipe.CreateFullOuter(ijoin)                
                    | JoinSmall -> SmallJoin(ijoin,JoinFullOuter)
                    | JoinLarge -> failwith "large column join not supported"
                
                let sink = new CollectorSink()
                joiner.Join leftSeq rightSeq (sink :> ISink).Sink   
                sink.GetSeq
                
            let mapAddFun (tuple:Primitives.Tuple) =              
                let leftSeq = tuple.GetTuples leftFieldName
                let rightSeq = tuple.GetTuples leftFieldName            
                let result = runOne leftSeq rightSeq            
                Datum.Tuples result            
                
            let schemafun (sch:Schema) =
                let leftSch = sch.InnerSchema leftFieldName
                let rightSch = sch.InnerSchema rightFieldName
                let leftSeq = TupleSequence.Create leftSch Seq.empty
                let rightSeq = TupleSequence.Create rightSch Seq.empty
                let result = runOne leftSeq rightSeq
                Column.Make(outFieldName,result.Schema |> SequenceElement)            
                                        
            MapAddPipe.Create(sprintf "OrderedColumnJoin(%s-%s)" leftFieldName rightFieldName,mapAddFun,schemafun)
        
    (* End of internal module *)
    (* Beginnning of public interfaces *)
    open Internal
    
    (* Groups results by the contents of a list of fields. The result will be a tuple sequence
       with a schema that has the grouped field, then the other columns be a Tuples
       stream. That is, group {Name:String, Age:int, Profession:String} by profession would give you
       {Profession:String, Tuples {Name:String, Age:int}}
    *)                                          
    let Group (joinSize:JoinSize) (groupFields:string list) (tupleField:string) =        
        let schemaGen isResult (input:Schema) =
             let collectSchema = input.DropColumns groupFields
             match isResult with
             | false -> collectSchema
             | true -> 
                let r = Column.Make(tupleField,Primitives.SequenceElement collectSchema)
                let l = groupFields |> 
                          List.map (fun field -> let col = input.GetColumn field
                                                 Column.Create (field,col.element, col.def))
                Schema.Create (l @ [r])
                                 
        let collector= {new ICollector with        
            member x.KeyGen (input:Tuple) = List.map input.GetDatum groupFields
            member x.Collect (sch:Schema) (input:Tuple) : Tuple =
                input.FillSchema sch                
            member x.Collected collectedSchema (keys:Datum list) (collected:TupleSequence) =
                let data = keys @ [ collected |> Primitives.Datum.Tuples ]
                Tuple.Create collectedSchema data
            member x.GetCollectSchema input = schemaGen false input
            member x.GetCollectedSchema input = schemaGen true input
            member x.IsPreOrdered key =             
                let rec ordCheck requirements orderings =
                    match requirements,orderings with
                    | [],_ -> true  // ran out of requirements, must be ordered
                    | _,[] -> false // ran out of ordered before requirements
                    | req :: rTl, (_,ord) :: oTl when req = ord -> ordCheck rTl oTl
                    | _ -> false
                ordCheck groupFields key.orderings
            member x.GetPostOrdering (initialKey:SchemaKey) = 
                let newOrd = 
                    if x.IsPreOrdered initialKey then 
                        (initialKey |> reorderSome (List_contains groupFields >> not)).orderings
                    else 
                        groupFields |> List.map (fun f -> Ascending,f)
                SchemaKey.Create(newOrd,true) // after a group by, keys are unique
            }
        Pipes.CollectorPipe.Create ("GroupBy", collector,joinSize)
        
    /// <summary> Create a pipe segment that does database-like joins.
    /// </summary>
    /// <param name="joinSize">The size of the join, of type <see cref="JoinSize" />
    ///     JoinLarge means use external storage to do the join -- use for
    ///               joining streams where both sides are large and unordered.
    ///     JoinSmall: The right side of the join is small enough to fit into memory,
    ///                so we can do the join in a streaming fashion.
    ///     JoinOrdered: The left and right parts of the join are ordered in such 
    ///                  a way that the join can be done in a streaming fashion.
    ///                  For this to hold, the left and right join fields have to
    ///                  be ordered in a key.
    ///     </param>
    /// <param name="joinDirection">Type of join -- inner, outer, etc</param>
    /// <param name="leftFields">Fields to join on from the left side of the pipe</param>
    /// <param name="rightFields">Fields to join on from the right side of the join</param>
    let public Join (joinSize:JoinSize,joinDirection:JoinDirection) (leftFields:string list) (rightFields:string list) =
            let ijoin = fieldJoin leftFields rightFields
            match joinSize,joinDirection with
            | JoinLarge,_ -> JoinPipe.Create(ijoin,joinDirection)        
            | JoinSmall,_ -> SmallJoin(ijoin,joinDirection)
            | JoinOrdered,JoinLeftOuter -> OrderedJoinPipe.CreateLeftOuter ijoin 
            | JoinOrdered,JoinFullOuter -> OrderedJoinPipe.CreateFullOuter ijoin 
            | JoinOrdered,JoinInner -> OrderedJoinPipe.CreateInner ijoin 
     
    // Adds the results of two fields and returns the left tuple updated with the 
    // value of the sum.       
    let private SumField (leftField,rightField) =
        fun (left:Tuple) (right:Tuple) ->
            let ld = left.GetDatum leftField
            let rd = right.GetDatum rightField
            let newD = Datum.Add ld rd
            left.UpdateValue leftField newD
            
    /// <summary> custom merge where we are merging two pipes and we don't change their schema.
    ///           If there is no collision between the left and right pipe (i.e. there are no overlaps
    ///           in the key space, then the tuples will get put directly into the result stream.
    /// </summary>
    /// <param name="joinSize">Size of the join</param>
    /// <param name="matchFields">Fields to use to create the join key</param>
    /// <param name="mergefun">Function that, given a desired schema and 
    ///                        two tuples, computes a tuple to use as the result.    
    let public JoinCollate 
        (joinSize:JoinSize) 
        (matchFields:string list)
        (mergefun:Schema->Tuple->Tuple->Tuple) =
        
        let ijoin = fieldMergeJoin matchFields mergefun
        match joinSize with
        | JoinLarge -> JoinPipe.Create(ijoin,JoinFullOuter)
        | JoinSmall -> SmallJoin(ijoin,JoinFullOuter)
        | JoinOrdered -> OrderedJoinPipe.CreateFullOuter ijoin         
    
    /// <summary>Special kind of merge where where tuples on the left side of the join
    ///          are considered to be the 'new' ones, and in the case of a key collision
    ///          with the right side of the pipe we keep the tuple from the left without
    ///          modification. For tuples that don't collide, functions like a mux merge
    ///          and passes tuple into the output stream.
    ///          Finally, the two pipes must be ordered in a way that the join can
    ///          be done in a streaming fashion </summary>
    let public JoinCollateOrdered keyFields =
        let joinFun sch left right = left            
        JoinCollate JoinOrdered keyFields joinFun
        
    /// <summary> Join pipe where in the case of a key collision we compute the sum
    ///           of a particular field, and update the left tuple with that value.
    /// <summary>
    let JoinCollateSum joinSize matchFields (addField) =
        JoinCollate joinSize matchFields (fun _ l r -> SumField (addField,addField) l r)
        
    /// <summary> Take two fields which are tuple sequences with the same schema, and merge them
    /// as in <cref="JoinCollateOrdered"/>. </summary>
    /// <param name="joinSize">Size of the join</param>
    /// <param name="leftField">Name of a tupleSequence that should go first</param>
    /// <param name="rightField">Name of the tupleSequence field that should have its tuples added last</param>
    /// <param name="outputField">What to call the resulting field
    /// <param name="matchFields">Field match keys</param>
    let SequenceFieldMerge (joinSize:JoinSize) (leftField,rightField,outputField) matchFields =
        let joinFun sch left right = left            
        let ijoin= fieldMergeJoin matchFields joinFun
        ColumnJoin joinSize (leftField,rightField,outputField) ijoin


    /// <summary> Combine two pipes and where the pipes match, collect the 
    ///           right side of the pipe in a column and add to the tuple from the left.
    /// </summary>
    let public JoinAsColumn columnName =
        let merge =
            {new IBlockingMerge<TupleSequence option> with
                member x.Name () = sprintf "JoinAsColumn(%A)"  columnName
                member x.InitialRightValue () = (None)
                member x.SchemaEffect left right = 
                    left.Add (Column.Make(columnName,SequenceElement right))
                member x.CollectRight tseq tseqOpt =
                    let collectorSeq =
                        match tseqOpt with
                        | None -> tseq.NewSeq tseq.Schema (seq [for tup in tseq.tseq do yield tup])
                        | Some collectSeq -> collectSeq .NewSeq collectSeq.Schema (seq [yield! collectSeq.tseq; yield! tseq.tseq])
                    Some collectorSeq
                member x.RunLeft (seqOpt : TupleSequence option) cmd f =
                    let rseq = seqOpt.Value
                    let dtm = Datum.Tuples rseq                    
                    let schemafun (sch:Schema) = Column.Make(columnName,SequenceElement rseq.Schema)
                    let addFun (tup:Tuple) = dtm
                    let map = MapAddPipe.Create("JoinAsColumnMap", addFun,schemafun)                                            
                    map.Run cmd f                    
                    ()
            }
        SequentialMergePipe.Create(merge)

    /// <summary> Join where we want to take an innner tuple sequence and map values to columns. The right
    ///           side of the pipe will have data describing how to map keys to columns in the new schema</summary>
    /// <param name="innerName">Name of inner tuple sequence field to explode. </param>
    /// <param name="innerKeyFields">Fields that form a key for 'exploding' the inner values</param>
    /// <param name="innerValueField">Field containing the value that will be the result in the exploded tuple</param>
    /// <param name="rightKeyFields">The fields from the right that describe the key</param>
    /// <param name="rightNameField">The field that has the name of the resulting column on the right</param>
    let ExplodeInner (innerName,innerKeyFields,innerValueField) (rightKeyFields,rightNameField) =
        
        {new IJoinPipe with            
            member self.Name () = sprintf  "ExplodeJoin" 
                            
            member self.Join (leftTseq:TupleSequence) (rightTseq:TupleSequence) (cont:PipeContinuation) : unit =
                let rightNameMap = new System.Collections.Generic.Dictionary<Datum list,string> ()
                let leftInnerSchema = leftTseq.schema.InnerSchema innerName
                let valueComp = leftInnerSchema.GetColumn innerValueField
                let rightNameList = new System.Collections.Generic.List<string> ()
                
                for (tup:Tuple) in rightTseq do 
                    let nameKey = rightKeyFields |> List.map tup.GetDatum
                    let name = tup.GetString rightNameField
                    rightNameMap.Add(nameKey,name)
                    rightNameList.Add(name)
                
                let fieldSchema = rightNameList |>
                                  Seq.map (fun fname -> Column.Create(fname,valueComp.element,valueComp.def)) |>
                                  List.ofSeq |> Schema.Create
                let newSchema = (leftTseq.schema.DropColumns [innerName]).Concat fieldSchema
                // now we have the mappings form the right hand side 
                let tseq = 
                    seq {
                        for (tup:Tuple) in leftTseq do
                            let innerSeq = tup.GetTuples innerName
                            let values = 
                                [for innerTup in innerSeq do
                                    let innerKey = innerKeyFields |> List.map innerTup.GetDatum
                                    let innerValue = innerTup.GetDatum innerValueField
                                    let colName = rightNameMap.[innerKey]
                                    yield colName,innerValue]
                                |> Map.ofList
                            let newTup =                                 
                                [for colName in rightNameList do
                                    match Map.tryFind colName values with
                                    | Some dtm -> yield dtm
                                    | None -> yield valueComp.def
                                ] |> Tuple.Create fieldSchema
                            yield Tuple.FillFromSchema newSchema [tup;newTup]                            
                    }

                TupleSequence.Create newSchema tseq |> cont
                ()                
        }