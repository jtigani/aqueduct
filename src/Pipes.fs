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

/// Module containing types and routines for constructing and running pipe systems.
module public Pipes =
    
    // Lowmem: set this to true if you are running into memory usage issues
    let lowMem = false
    
    open Aqueduct.Primitives
    open Aqueduct.Internal
    open Aqueduct.Internal.QueueManager
    open Aqueduct.Internal.Queue
    
    exception PipeRuntime of string    
    
    /// thrown by a sink to signal that no more needs to be computed         
    exception SinkDone
      
    /// Basic pipe interface representing a pipeline stage. 
    type IPipe = 
        
        /// each pipe should have a name 
        abstract Name: unit -> string
                
        /// Run the pipeline stage. Given a sequence of tuples,
        /// returns a sequence of tuples. Note that sequence evaluation 
        /// may be lazy. 
        abstract Run: TupleSequence->PipeContinuation->unit
        
        /// Given an input schema, returns a new schema 
        abstract SchemaEffect: Schema -> Schema
        
    /// Basic pipe interface representing a pipeline stage that publishes a name
    and IPublishPipe = 
        
        /// each pipe should have a name 
        abstract Name: unit -> string
        
        /// Run the pipeline stage. Given a sequence of tuples,
        /// returns a sequence of tuples. Note that sequence evaluation 
        /// may be lazy. 
        abstract Run: TupleSequence->PipeContinuation list->unit
        
    /// Each join has two side -- left and right. Left side is the 'main' pipe, right side is generally the 
    /// subordinate side of any join. AnySide is a "don't care" option
    and JoinSide = LeftSide | RightSide | AnySide     
    
    /// Basic pipe interface representing a pipeline stage that takes multiple inputs. 
    and IJoinPipe =                  
        /// Name of the pipe                
        abstract Name: unit -> string
                        
        /// Joins tuple sequences. Note this will be called just once, and the two sequences
        /// will be lazy representations of the whole tuple sequence. 
        abstract Join: TupleSequence -> TupleSequence -> PipeContinuation->unit
    
    /// Interface representing a source of tuples -- this can be a .csv file 
    /// a .tab file, a database, a socket, etc.
    and ITap =        
        abstract Tap: unit -> TupleSequence
    
    /// Interface representing a final destination for tuples (generally storage).
    /// This can be a .csv file, an xml file, or a database 
    and ISink =
        /// Name of the sink                
        abstract Name: unit -> string
        abstract Sink: TupleSequence -> unit
      
    
    /// Specialized pipe that collects tuples. 
    type ICollector =
        /// Unique identifier for tuples. We collect based on this key. Note that
        /// any key with different value can be collected in parallel.            
        abstract KeyGen: Tuple -> Datum list
                     
        /// A function returning the tupl to collect given the tuple read. Schema arg is expected schema 
        abstract Collect: Schema->Tuple -> Tuple
       
        /// Return the schema for the individual tuples collected by Collect (given the input schema) 
        abstract GetCollectSchema: Schema -> Schema
  
        /// Once all of the tuples matching a key have been identified,
        /// pass those as a sequence to the Collected function where
        /// a single tuple is returned. The Schema that is passed is the
        /// collected schema 
        abstract Collected: Schema -> Datum list -> TupleSequence -> Tuple
 
        /// Return the schema for the tuples collected by Collected (given the input schema) 
        abstract GetCollectedSchema: Schema -> Schema
         
        /// Indicates whether this pre-ordering is acceptable to be already be ordered for grouping 
        abstract IsPreOrdered: SchemaKey -> bool                                
        
        /// describes ordering after the collection is run 
        abstract GetPostOrdering: SchemaKey -> SchemaKey

    /// Describes a join type -- inner, full outer, or left outer. These are equivalent to sql
    /// join types.
    type JoinDirection = JoinLeftOuter | JoinInner | JoinFullOuter
                                                                            
       
    /// Pipe that takes two tuple sequences and merges them, calling one before the other. 
    type IBlockingMerge<'a> =
        /// Returns the intiial value for the right (secondary) side of the computation 
        abstract InitialRightValue: unit -> 'a
        
        /// For the right (secondary side), collect objects into a function of type 'a. Note
        /// that this may be called multiple times. We pass the tuple sequence and
        /// the current value and it should return the new value. 
        abstract CollectRight:TupleSequence->'a->'a
                        
        /// Called after the right sequence has completed, and gives the commands given to the left sequence.
        /// we also pass the final value of the right results, and pass the continuation to call the next stage 
        abstract RunLeft:'a->TupleSequence->PipeContinuation->unit
        
        /// Effect them merge has on the schema, given the original schema and the collected schema
        abstract SchemaEffect: Schema->Schema->Schema
        
        /// name of this merge 
        abstract Name: unit-> string
        
        
    /// Concrete imlementation of a tap function given a function that creates a sequence of tuples.
    /// Also can be used to connect pipes to a tap.                           
    type Tap  = 
        val private f:unit->TupleSequence
        private new (f: unit -> TupleSequence) = {f=f}
        static member Create (f: unit -> TupleSequence) =
            new Tap(f) :> ITap
        /// Creates a tap that iterates a tuplesequence when run.
        static member FromSeq tseq = Tap.Create (fun () -> tseq)
        interface ITap with 
            member x.Tap () = x.f ()
                
    /// type representing a concrete implementation of an ISink 
    type Sink (n:string, f: TupleSequence -> unit)  = 
        interface ISink with 
            member x.Sink tseq = f tseq 
            member x.Name () = n
        static member Create (n:string, f: TupleSequence -> unit) =
            new Sink (n, f) :> ISink
       
    /// Cardinality of a tuple sequence, used for joins and group by. Note that
    /// the important cardinality is generally the size of the right pipe.
    /// JoinLarge means the tuple sequence may be too large to fit in memory
    /// and it isn't ordered in a way that it the join can be done in sequence,
    /// JoinSmall means that the whole sequence will fit into memory, while
    /// JoinOrdered means that the sequence should be ordered in a way that 
    /// a join can be done in stream.
    type JoinSize = JoinLarge | JoinSmall | JoinOrdered    
        
    let rec internal safeStart (thr:System.Threading.Thread) =
        if thr.ThreadState = System.Threading.ThreadState.Unstarted then
            lock thr (fun _ -> if thr.ThreadState = System.Threading.ThreadState.Unstarted  then thr.Start ())
        
    /// Pipesystems are built out of pipe stages.
    /// Normal stages are normal pipe segmants
    /// Join stages connect two PipeStages
    /// All PipeStages must start with a SubscribeStage which
    /// describes where they get their data. (either a tap or
    /// a publish can furnish the data)
    /// PublishStages publish a name, and any other pipestages
    /// can subscribe to that name to get a copy of the data.
    type PipeStage = 
        | JoinStage of IJoinPipe * PipeStage * PipeStage 
        | NormalStage of IPipe * PipeStage 
        | SubscribeStage of string
        | PublishStage of string * PipeStage
            
    let private NormalBuilder pipe = fun stage -> NormalStage (pipe,stage)
    let private JoinBuilder pipe left = fun stage -> JoinStage(left,pipe,stage)
    
    /// Schema key transformation function indicating that a schema key is untouched
    let orderPreserving (ord:SchemaKey) = ord
    
    /// Schema key transformation function indicating that any ordering is lost.
    let reordering ord = SchemaKey.Empty
    
    /// Schema key transofrmation function indicating that any ascending or descending ordering
    /// changes to 'grouped' only, which means that all like keys are grouped together but no
    /// assumptions can be made about whether keys increase or decrease in order
    let orderedToGrouped (key:SchemaKey) = 
        let ord = [for ord in key.orderings -> match ord with (dir,field) -> (Grouped,field)] 
        SchemaKey.Create(ord,key.isUnique) // 
    
    /// Schema key transformation function where a predicate can be povided to indicate which 
    /// fields will be reordered.
    let reorderSome checkName (key:SchemaKey) =         
        let rec orderFunInner ord =         
            match ord with 
            | (dir,field) :: tl -> 
                if checkName field then orderFunInner tl else (dir,field) :: (orderFunInner tl)
            | [] -> []
        let ords = orderFunInner key.orderings
        SchemaKey.Create(ords,key.isUnique)
          
    let private PipeStages (initial:PipeStage) (builderList:(PipeStage->PipeStage) list) : PipeStage =
        let rec pipeStagesInner lst acc =
            match lst with
            | [] -> acc
            | hd :: tl -> (pipeStagesInner tl (hd acc))
        pipeStagesInner builderList initial
        
    /// Adds a list of pipes to a pipestage
    let NormalPipes (initial:PipeStage) (pipes:IPipe list) = 
        let builders = List.map NormalBuilder pipes
        PipeStages initial builders
    
    //
    // Operators to allow you to easily chain together pipe stages.
    //
     
 
    /// Subscribe to a named tap. The tap can be directly provided
    /// by an external tap or it can be published by another stage.
    let ( *> ) (q:string) (p:IPipe) : PipeStage = NormalStage(p,SubscribeStage q)
    /// Adds an IPipe stage to a pipeline
    let (<*>) (q:PipeStage) (p:IPipe) : PipeStage = NormalStage(p,q)
    /// Concatenates a pipeline to another. The right argument is a function that
    /// creates the new pipeline given the lefthand pipeline
    /// e.g. let right p = p <*> Zzz;; let stages = Xxx <*> Yyy <*@*> right;;)  
    let (<*@*>) (q:PipeStage) (p:PipeStage->PipeStage) : PipeStage = p q
    /// Join two pipelines, such that two pipelines have one output. One example of a
    /// join is a SQL-style relational join. 
    /// The pipeline from the left of the operator is the left side of the join
    /// and the pipeline from the 'right' argument is the right side of the join.
    let (<**>) (left:PipeStage) ((p,right):(IJoinPipe*PipeStage)) : PipeStage = JoinStage(p,left,right)
    /// Publish a name of a stage
    let (<%>) (q:PipeStage) (p:string) : PipeStage = PublishStage(p,q)    
    /// Add a list of pipes in sequence
    let (<^>) (q:PipeStage) (p:IPipe list) : PipeStage = List.fold (fun stg pipe ->  NormalStage(pipe,stg)) q p
            
         
    /// Join a number of join pipes in sequence   
    let MultiJoin (rights: (IJoinPipe*PipeStage) list) (left:PipeStage)  = List.fold (fun lhs (pipe,rhs) -> JoinStage(pipe,lhs,rhs)) left rights
    /// Connect a number of pipes in sequence
    let MultiPipe (pipes: IPipe list) (left:PipeStage) = List.fold (fun lhs pipe -> NormalStage(pipe,lhs)) left pipes
    
    /// Given a pipestage, return the name of the tap. If there are joins, follows the left side of the join.
    let rec GetTapName stage =
        match stage with
        | SubscribeStage str -> str
        | JoinStage (_,left,_) -> GetTapName left
        | NormalStage (_,next) -> GetTapName next
        | PublishStage (_,next) -> GetTapName next

    /// Get the name of this particular stage.
    let GetStageName stage =
        match stage with
        | SubscribeStage name -> "\"" + name + "\""
        | JoinStage (pipe,_,_) -> pipe.Name()
        | NormalStage (pipe,_) -> pipe.Name()
        | PublishStage (name,_) -> name
        
            
    let internal createStorage joinSize multiStorage = 
        let createFun = 
            match joinSize with 
            | JoinSmall -> MetricsStorage.MetricsStorageMemory.Create 
            | JoinLarge -> MetricsDb.MetricsStorageTable.Create   
            | JoinOrdered -> MetricsStorage.MetricsStorageNone.Create // this will fail if someone tries to use it.
        if multiStorage then
            //createFun ()
            MetricsStorage.MultiStorage.Create (4,createFun)
        else
            createFun ()


    let private handleEx ex msg=        
        let severe = 
            match ex with
            | SinkDone -> false
            | SchemaValidation str
            | OrderingValidation str ->  fprintfn System.Console.Error "Exception %s %s" str msg; true
            | exn -> fprintfn System.Console.Error "Exception %s %s" (exn.ToString ()) msg; true
        if severe then
            fprintfn System.Console.Error "Exception: %s" ex.StackTrace
        
    /// FilterResult is used in a filter pipe -- a filter function can return 
    /// accept, which means that you want to keep the tuple, drop which means
    /// don't keep the tuple (i.e. filter it out), and done which means that
    /// no more tuples should be kept (this is a short circuit function).                
    type FilterResult = FilterAccept | FilterDrop | FilterDone
    
    /// Pipe that applies a filter (IFilter) to a stream of Tuples 
    type FilterPipe = 
        private {filter:Tuple->FilterResult;name:string}
    with
        /// Use this to create a filter pipe from an IFilter 
        static member Create(name,(filter:Tuple->FilterResult)) = {filter=filter;name=name} :> IPipe
        interface IPipe with
            member self.Name () = sprintf "Filter(%s)" self.name            
            member self.SchemaEffect sch = sch
            member self.Run (aseq:TupleSequence) f = 
                // filter doesn't change the schema                    
                let tseq = seq {
                    let isDone = ref false
                    let enum = aseq.tseq.GetEnumerator ()
                    while (!isDone) = false && enum.MoveNext() do
                        let tup = enum.Current 
                        match self.filter tup with
                        | FilterAccept -> yield tup
                        | FilterDrop ->()
                        | FilterDone -> isDone := true 
                }
                aseq.NewSeq aseq.Schema tseq  |> f
                
    
    /// Create a filter given a function that takes a tuple and returns whether it should be kept in the stream 
    let SimpleFilter (filter : (Tuple -> bool)) =        
        FilterPipe.Create ("filter",(fun tup -> if (filter tup) then FilterAccept else FilterDrop))
                    
    /// Pipe stage type that forms that basis of pipes that operate on
    /// each tuple in order. Requires thre functions: a schema translation function
    /// describing how this pipe stage changes an input schema, a key translation function
    /// describing what the effect on pipe ordering will be, and a tuple translation function
    /// that maps input tuples to output tuples.
    type MapPipe =
        private {map:Tuple->Tuple;name:string;keyEffect:SchemaKey->SchemaKey;schemaEffect:Schema->Schema}
    with
        
        /// Create the map pipe
        static member Create(name,map:Tuple->Tuple,keyEffect,schemaEffect) = 
            {map=map;name=name;keyEffect=keyEffect;schemaEffect=schemaEffect}:> IPipe
        
        interface IPipe with
            member self.Name () = sprintf "Map(%s)" self.name
            member self.SchemaEffect sch = self.schemaEffect sch            
            member self.Run aseq f =
                let oschema = (self.schemaEffect aseq.Schema).SetKey (self.keyEffect aseq.Schema.Key)
                let tseq = seq {for a in aseq.tseq do 
                                    let tup = self.map a
                                    yield tup}
                let tupseq = 
                    Primitives.TupleSequence.CreateKeyed oschema tseq
                tupseq |> f

    /// Similar to a map pipe, but passes the expected result schema to the map function,
    /// prevents an unnecessary (expensive) schema creation operation every tuple. 
    /// This is preferred to MapPipe.
    type FastMapPipe =
        private  {map:Schema->Tuple->Tuple;name:string;schemaEffect:Schema->Schema}
    with
        /// Create the fast map pipe
        static member Create(name,map:Schema->Tuple->Tuple,schemaEffect) = 
            {map=map;name=name;schemaEffect=schemaEffect}:> IPipe
        
        interface IPipe with
            member self.Name () = sprintf "Map(%s)" self.name
            member self.SchemaEffect sch = self.schemaEffect sch            
            member self.Run aseq f =
                let oschema = self.schemaEffect aseq.Schema
                let tseq = seq {for a in aseq.tseq do 
                                    let tup = self.map oschema a
                                    yield tup}
                let tupseq = 
                    Primitives.TupleSequence.Create oschema tseq
                tupseq |>  f
                
    /// Pipe stage where a a column is added and order is preserved
    type MapAddPipe =
        private {mapAdd:Tuple->Datum;name:string;newColType:Schema->Column}
    with
        /// Given an IMap, return an IPipe 
        static member Create(name,mapAdd:Tuple->Datum,newColType:Schema->Column) = 
            {mapAdd=mapAdd;name=name;newColType=newColType}:> IPipe        
        interface IPipe with
            member self.Name () = sprintf "MapAdd(%s)" self.name            
            member self.SchemaEffect (sch:Schema) = 
                let comp = self.newColType sch
                match comp.element with
                | SequenceElement innerSch  -> 
                    let tuples = comp.def |> Datum.ExtractTuples 
                    if innerSch <> tuples.Schema then
                        raise <| SchemaValidation (sprintf "mismatch between inner schema %A and outer: %A" innerSch tuples.Schema)
                    else if not <| Seq.isEmpty tuples.tseq then
                        raise <| SchemaValidation (sprintf "mismatch between inner schema %A and outer: %A" innerSch tuples.Schema)
                    else 
                        let comp' = comp.SetElement  (SequenceElement (innerSch.SetKey innerSch.Key))
                        sch.Add comp'
                | _ ->  sch.Add comp
            member self.Run aseq f =
                let oschema = (self :> IPipe).SchemaEffect aseq.Schema
                let tseq = 
                    seq {
                        for tupIn in aseq.tseq do 
                            let dtm = self.mapAdd tupIn
                            let dtm' = 
                                match List.head oschema.Elements with
                                | SequenceElement sch ->                                                                                            
                                    // cache inner tuple sequence 
                                    let tseq = Datum.ExtractTuples dtm
                                    let tseq' = tseq.NewSeq tseq.Schema (tseq.tseq |> List.ofSeq)                                                
                                    tseq' |> Datum.Tuples
                                | _ -> dtm
                            yield Tuple.Create oschema (dtm' :: tupIn.values) 
                    }
                aseq.NewSeq oschema tseq |>  f
        
    /// Type that turns an ICollector into a pipe (IPipe) 
    and  CollectorPipe =
        val private collector : ICollector
        val private name : string        
        val private dbTable:MetricsStorage.IMetricsStorage        
        val private joinSize:JoinSize
        private new (name,collector,joinSize) = {collector=collector;name=name;joinSize=joinSize;dbTable = createStorage joinSize true}
    with
        /// Given an ICollector, return an IPipe 
        static member public Create(name,collector,joinSize) = new CollectorPipe(name,collector,joinSize) :> IPipe
        
        member private self.GenerateSeq (fromTseq:TupleSequence) : TupleSequence =                                    
                let tupleSchema = self.collector.GetCollectSchema fromTseq.Schema
                let resultSchema = (self :> IPipe).SchemaEffect fromTseq.Schema                
                
                let tseq =                 
                    seq {                        
                        let lastKey:Datum list ref = ref []
                        let collection = new System.Collections.Generic.List<Tuple> ()
                        
                        for tup in fromTseq.tseq do
                                                        
                            let currentKey = self.collector.KeyGen tup               
                            
                            let tup = self.collector.Collect tupleSchema tup  // The tuple to be collected (which may not be the original)
                        
                            if (collection.Count = 0) then                                
                                // this is the first time through.
                                lastKey := currentKey
                                ()
                            else if (currentKey <> !lastKey) then
                                // the keys are ordered, but the key has changed, so
                                // we can release the collected tuples for the old key.                                
                                let tt = TupleTable.CreateFromList tupleSchema collection
                                //let tseq = TupleSequence.Create tupleSchema ((collection.ToArray ()) |> Array.copy)
                                
                                let result = self.collector.Collected resultSchema (! lastKey) (tt.GetTseq ())
                                collection.Clear ()
                                
                                lastKey := currentKey
                                yield result
                            collection.Add tup
                                
                        // Release the remainder (if there is one)
                        if (collection.Count <> 0) then
                            let tt = TupleTable.CreateFromList tupleSchema collection
                            //let tseq = TupleSequence.Create tupleSchema (collection.ToArray ())
                            let result = self.collector.Collected resultSchema (! lastKey) (tt.GetTseq ())
                            yield result
                    }                
                TupleSequence.Create resultSchema tseq 
                
        member self.Spawn () = CollectorPipe.Create (self.name,self.collector,self.joinSize)
        /// This is the pipe key gen, which determines how to parallelize. Note that 
        /// things that have different keys can be run in parallel since they will
        /// never need to be collected toegether 
        member self.KeyGen tup =  self.collector.KeyGen tup
        
                                                                    
        interface IPipe with       
            member self.Name () = sprintf "Collect(%s)" self.name            
            member self.SchemaEffect sch = (self.collector.GetCollectedSchema sch).SetKey (self.collector.GetPostOrdering sch.Key)
            
            /// In order to collect the tuples, we need to see all of them first if they are not in
            /// a well-known order. Because we may not be able to store all of them in memory, we
            /// serialize them to a database (MetricsStorageTable) with a key defined by the ICollector,
            /// and then read them back in key order. If the tuples are already ordered then we do not
            /// have to go through that storage step. 
            member self.Run aseq f =
                match self.joinSize with
                | JoinSmall 
                | JoinLarge -> self.dbTable.AddSeq self.KeyGen aseq
                               let tseq = self.GenerateSeq (self.dbTable.Read ())
                               f tseq
                               self.dbTable.Close ()
                | JoinOrdered when (self.collector.IsPreOrdered aseq.Schema.Key ) ->  self.GenerateSeq aseq |> f
                | JoinOrdered ->  failwith (sprintf "%s not really preordered" (aseq.Schema.ToString()))

    /// Pipe that implements a type of join where the right side of the pipe
    /// is run and collected before the left side continues.         
    type SequentialMergePipe<'a> (merge:IBlockingMerge<'a>) =                 
        interface IJoinPipe with                        
            member x.Name () = sprintf "SequentialMerge(%s)"  (merge.Name ())
            //member x.SchemaEffect side sch = merge.SchemaEffect side sch
            member x.Join left right (cont:PipeContinuation)=
                let rightResult = merge.CollectRight right (merge.InitialRightValue ())
                merge.RunLeft rightResult left cont                
        member x.SchemaEffect left right = merge.SchemaEffect left right
                                                              
        /// Creates an IJoinPipe given an ISequentialMerge                                      
        static member Create<'a> merge = new SequentialMergePipe<'a> (merge) :> IJoinPipe        

    /// Chops a pipe up into mini-sequences that can fit into memory 
    let internal chopSequence interval runFun expediteFun =
        fun (aseq:TupleSequence) ->                                  
            let ii = ref 0
            let empty = ref true
            let dummy = (Tuple.Create Schema.Empty [])
            let lst = new System.Collections.Generic.List<Tuple> (interval:int)
            let curInterval = ref 1
            for tup in aseq.tseq do                        
                empty := false
                lst.Add tup
                ii := !ii + 1
                if !ii = !curInterval || expediteFun () then
                    let tseq = TupleSequence.Create aseq.Schema (lst.ToArray () |> Array.copy)
                    lst.Clear ()
                    let tcmd = tseq |> runFun                    
                    ii := 0
                    curInterval := min (!curInterval * 2) interval 
            if !empty || !ii <> 0 then                  
                let newLst  = lst.GetRange(0,!ii)
                newLst |> TupleSequence.CreateUnsafe aseq.Schema |> runFun               
            
    /// FunctionPipe creates a pipe stage that computes a function. It threads an accumulator through
    /// the tuples, and the result is visible from the Resumt member function after the pipe is run.
    type FunctionPipe<'a> =        
        val mutable private result:'a
        val private visitor:(Tuple->'a->'a)
                        
        private new(initial,visitor) = {
            visitor=visitor
            result = initial
            }        
    with    
        
        static member Create (initial:'a,visit:(Tuple->'a->'a)) =            
            new FunctionPipe<'a>(initial,visit)
        member x.Result () = x.result
        member private  self.ComputeResult (aseq:Primitives.TupleSequence) =
            self.result <- Seq.fold (fun acc cur -> self.visitor cur acc) self.result aseq.tseq 
        interface IPipe with
        
            member self.Name () = "function"             
            member self.SchemaEffect sch = sch
            member self.Run aseq f =                
                let tseq = 
                    seq { for tuple in aseq do 
                            self.result <- self.visitor tuple self.result
                            yield tuple }
                aseq.NewSeq aseq.Schema tseq |> f                

    
    type ThreadBufferPipe (name:string) =
        let queue = BoundedBlockingQueue<PipeCmd>.Create 0x10
        let loop cont () =
            try 
                System.Threading.Thread.CurrentThread.Name <- sprintf "ThreadBuffer[%s]" name
                let innerLoop () =
                    let firstCmd  = queue.Dequeue ()
                    match firstCmd with 
                    | InputCmd firstTseq  -> 
                        let tseq = 
                            seq {
                                yield! firstTseq.tseq
                                let isDone = ref false
                                while !isDone = false do
                                    let cmd = queue.Dequeue ()
                                    match cmd with 
                                    | InputCmd tseq -> yield! tseq.tseq
                                    | DoneCmd -> isDone := true
                            }
                        let collected = TupleSequence.Create firstTseq.Schema tseq
                        cont collected
                    | _-> failwith "expected input cmd before done cmd"

                innerLoop ()
            with 
                | :? SinkDone -> ()
                | ex -> 
                    handleEx ex ("on buffer thread " + name)                
                    reraise ()
                                                               
        static member Create(name) = new ThreadBufferPipe(name) :> IPipe

        interface IPipe with
            member self.Name () = sprintf "Buffer[%s]" name                        
            member o.SchemaEffect sch = sch
            member self.Run aseq cont =
                let thread = new System.Threading.Thread (loop cont) 
            
                safeStart thread                                
                // chop the sequence into smaller size pieces, making sure that the sequence gets enumerated
                // on the current thread rather than the join thread. 
                chopSequence 0x400 (InputCmd >> queue.Enqueue) queue.IsEmpty aseq
                queue.Enqueue DoneCmd
                queue.BeginClose ()
                ()

    type internal BalancedPublishPipe (qm:QueueManager,name:string,n:int) =                                                                                                                
        let maxLength = if lowMem then 0x4 else 0x20
        let queues = qm.NewQueueGroup n maxLength (sprintf "PublishB.%d[%s]" n name)
        let loop i cont ()=             
            try 
                System.Threading.Thread.CurrentThread.Name <- sprintf "PublishB.%d/%d[%s]" i n name
                let innerLoop () =
                    let cmd = queues.Dequeue i
                    match cmd with 
                    | DoneCmd -> failwith "done before run in publish"
                    | InputCmd tseq ->
                        let newSeq = 
                            seq {
                                yield! tseq.tseq
                                let isDone = ref false
                                while !isDone = false do
                                    let newCmd = queues.Dequeue i
                                    match newCmd with 
                                    | InputCmd curTseq ->
                                        yield! curTseq
                                    | DoneCmd -> isDone := true
                            }
                        let collected = tseq.NewSeq tseq.Schema newSeq
                        collected |> cont

                innerLoop ()
            with 
                | :? SinkDone -> ()
                | ex -> 
                    handleEx ex ("on publish thread " + System.Threading.Thread.CurrentThread.Name)
                    reraise ()
                                        
        static member Create(qm,name,npipes) = new BalancedPublishPipe(qm,name,npipes) :> IPublishPipe                

        interface IPublishPipe with
            member self.Name () = sprintf "PublishB.%d[%s]" n name            
            member self.Run aseq flist =
                // chop the sequence into smaller size pieces, making sure that the sequence gets enumerated
                // on the current thread rather than the join thread.
                let conts = flist |> List.toArray
                let threads = Array.init n (fun ii -> let thr = new System.Threading.Thread (loop ii conts.[ii]) in thr)
                let sinkFun sinkCmd =                    
                    for ii in 0 .. (n - 1) do                    
                        safeStart threads.[ii]
                    queues.Enqueue (InputCmd sinkCmd)
                let expedite () =
                    queues.InternalQueues |> Array.tryFind (fun q -> q.IsEmpty ()) <> None
                chopSequence 0x100 sinkFun expedite aseq
                queues.Enqueue DoneCmd
                                        
    let compressTseq (tseq:TupleSequence)  : TupleSequence =
        let compressed = Serialization.compressTuples tseq
        let resultingSeq =
            seq {
                yield! Serialization.decompressTuples compressed
            }
        tseq.NewSeq tseq.Schema resultingSeq
    
    let internal JoinController (qm:QueueManager) name (joinPipe:IJoinPipe) : IPipe * IPipe =
        let maxLength = if lowMem then 0x4 else 0x20
        let joinQ= Array.init 2 (fun idx -> qm.NewQueue maxLength (sprintf "JoinQ:%s.%d" name idx))
        let getIdx side = match side with LeftSide -> 0 | RightSide -> 1 | AnySide -> failwith "bad idea"
        let getSide idx = match idx with 0 -> LeftSide | 1 -> RightSide | _ -> failwith "unexpected queue index"        
            
        let buildSeq initial idx = 
            seq {
                yield! initial
                let isDone = ref false
                while !isDone  = false do                
                    let cmd = joinQ.[idx].Dequeue ()
                    match cmd with 
                    | InputCmd tseq -> yield! tseq
                    | DoneCmd -> isDone := true
            }
                    
        let loop cont ()  =             
            try 
                System.Threading.Thread.CurrentThread.Name <- sprintf "Join %s +(%s)" name (joinPipe.Name ())
                let leftCmd = joinQ.[getIdx LeftSide].Dequeue ()
                let rightCmd = joinQ.[getIdx RightSide].Dequeue ()
                match leftCmd,rightCmd with
                | InputCmd ltseq, InputCmd rtseq ->
                    let lseq = buildSeq ltseq.tseq (getIdx LeftSide)                                                
                    let rseq = buildSeq rtseq.tseq (getIdx RightSide)
                    let collectedLeft = ltseq.NewSeq ltseq.Schema lseq
                    let collectedRight = rtseq.NewSeq rtseq.Schema rseq
                    joinPipe.Join collectedLeft collectedRight cont
                | _,_ -> failwith "Seen done before run in join pipe."
                                        
            with 
                | :? SinkDone -> ()
                | ex -> 
                    handleEx ex (" on join thread " + System.Threading.Thread.CurrentThread.Name)
                    fprintfn System.Console.Error "Exception: %s" ex.StackTrace 
                    reraise ()
                        
                    
        let createPipe (side:JoinSide) = 
            let q = joinQ.[getIdx side]
            
            {new IPipe with
                member self.Name () = sprintf "%s_%A" (joinPipe.Name ()) side                
                member self.SchemaEffect sch = sch // the schema on this pipe is not affected.
                member self.Run aseq pipeCont =
                    if side = LeftSide then 
                        let thread = new System.Threading.Thread(loop pipeCont)
                        // chop the sequence into smaller size pieces, making sure that the sequence gets enumerated
                        // on the current thread rather than the join thread.
                        safeStart thread
                    else 
                        // send an empty sequence down the right (dummy) side
                        Seq.empty |> TupleSequence.Create Schema.Empty |> pipeCont

                    let enq = (fun tseq -> q.Enqueue (InputCmd tseq))
                    let expedite () = q.InternalQueue.IsEmpty ()
                    let chopper = chopSequence 0x100 enq expedite aseq
                    q.Enqueue DoneCmd                    
            }
        (createPipe LeftSide, createPipe RightSide)                               
       
    /// Runs a pipe to a sink as well as through the pipeline. 
    let PipeSink (sink:ISink) : IPipe =       
       {new IPipe with
           member self.Name () = sprintf "pipeSink_%A" sink           
           member self.SchemaEffect s = s
           member self.Run pipeCmd nextStage = 
              sink.Sink pipeCmd              
              nextStage pipeCmd                              
       }
    
    /// This type should be internal, but fsharp won't let it be. This will apparently be fixed in the next
    /// fsharp release.
       
    type CompletionEvent = System.Threading.ManualResetEvent 
    module internal PipeImpl = 
        
        type CompletionType = NotCompleted | CompleteOk | CompleteException of exn
        type DispatchState = {mutable running:bool;mutable hasStarted:bool;mutable state:CompletionType} 
                with static member Create() = {running=false;hasStarted=false;state=NotCompleted}    
                     static member CreateUnrestricted() = {running=false;hasStarted=true;state=NotCompleted} 
    
        type DispatchStage = | DispatchSink of DispatchState * ISink * CompletionEvent
                             | DispatchSingle of DispatchState * IPipe * DispatchStage 
                             | DispatchPublish of DispatchState * IPublishPipe * DispatchStage list 
        with
                        
            member x.GetState () = 
                match x with 
                | DispatchSingle (state,_,_)                
                | DispatchPublish(state,_,_)                 
                | DispatchSink (state,_,_)  -> state   
            member x.WaitForSinks () =
                match x with 
                | DispatchSingle (_,_,next) -> next.WaitForSinks ()
                | DispatchPublish(_,_,lst) -> lst |> List.iter (fun next -> next.WaitForSinks ())
                | DispatchSink (_,_,doneEvent)  -> doneEvent.WaitOne () |> ignore

        let rec dispatchException ex  (x:DispatchStage) =
            match x with
                | DispatchSingle (_,_,next) -> dispatchException ex next 
                | DispatchPublish(_,_,lst) ->  lst |> List.iter (dispatchException ex)
                | DispatchSink (_,_,event)  -> event.Set () |> ignore
            
        let private comb state cont tseq=
            if state.state <> NotCompleted then failwith "already done"
            state.hasStarted <- true
            state.running <- true
            cont tseq
            state.state <- CompleteOk
            state.running <- false
                    
        let rec buildDispatchPath (x:DispatchStage) =           
            let cont =
                match x with
                | DispatchSingle (state,ip,next) ->
                    let nextCont = buildDispatchPath next
                    let run = ip.Run
                    (fun cmd -> run cmd nextCont)
                | DispatchPublish(state,id,lst) -> 
                    let nextConts = lst |> List.map buildDispatchPath
                    let run = id.Run
                    (fun cmd -> run cmd  nextConts)
                | DispatchSink (state,sink,event)  -> 
                    (fun cmd -> sink.Sink cmd;event.Set () |> ignore)
            let state = x.GetState ()
            comb state cont
    
        let buildDispatchDag (qm:QueueManager) (inputStages:(ISink * PipeStage) list) (taps:Map<string,ITap>) : (string * ITap * DispatchStage) list=
                                            
            // remove the joins from the input stages by creating join controllers to coordinate them.
            // the result after this will be effectively a tree.
            let rebuildInputStages (sink,stage) =
                let rec tapName pipeStage =
                    match pipeStage with
                    | SubscribeStage name -> name
                    | NormalStage (ipipe,nextStage) -> tapName nextStage
                    | JoinStage (ipipe,nextLeft,nextRight) -> sprintf "<**> %s %s" (tapName nextLeft) (tapName nextRight)
                    | PublishStage (name,nextStage) -> tapName nextStage//sprintf "%s <$> %s" (tapName nextStage) name
                    
                let rec loop pipeStage acc = 
                    match pipeStage with 
                    | SubscribeStage name -> SubscribeStage name, acc
                    | NormalStage (ipipe,nextStage) -> 
                        let inner,res = loop nextStage acc
                        NormalStage(ipipe, inner), res
                    | JoinStage (jp,left,right) ->         
                                    
                        let leftJp,rightJp = JoinController  qm (tapName pipeStage) jp
                        let innerR, acc' = loop right acc
                        let stageR = NormalStage (rightJp, innerR)
                        let sinkR = Sink.Create("", (fun _ -> ()))
                        let innerL, acc'' = loop left acc'
                        let stageL = NormalStage (leftJp, innerL)                                        
                        (stageL, (sinkR,stageR) :: acc'')                                        
                    | PublishStage (name,nextStage) -> 
                        let inner,res = loop nextStage acc
                        PublishStage (name, inner),res
                let stg,res = loop stage []
                (sink,stg) :: res                            
            let inputStages = inputStages |> List.map rebuildInputStages |> List.concat
                                    
            let getNamedNames root =
                let rec loop pipeStage =
                    match pipeStage with 
                    | SubscribeStage name -> []
                    | NormalStage (_,nextStage) -> loop nextStage 
                    | JoinStage (_,left,right) -> loop left @ loop right
                    | PublishStage (name,nextStage) -> name :: loop nextStage 
                loop root
            let namedNames = inputStages |> List.map (snd >> getNamedNames) |> List.concat                                                        
            let buildTapNames (sink,root)=
                let rec loop pipeStage =
                    match pipeStage with 
                    | SubscribeStage name -> name,(root,sink)
                    | NormalStage (_,nextStage) -> loop nextStage 
                    | JoinStage (_,left,right) -> failwith "all join stages should be gone"
                    | PublishStage (_,nextStage) -> loop nextStage 
                loop root
            let tapNames = inputStages |> List.map buildTapNames
            let tapSinks (name:string) = tapNames |> List.filter (fun (n,(stg,snk)) -> n = name) |> List.map snd
            let buildDependencies (sink,pipeStage) =
                let rec loop pipeStage = 
                    match pipeStage with 
                    | SubscribeStage name -> []
                    | NormalStage (_,nextStage) -> loop nextStage
                    | JoinStage (_,left,right) -> failwith "all join stages should be gone"
                    | PublishStage (name,nextStage) -> (tapSinks name |> List.map snd) @ loop nextStage
                (sink,loop pipeStage)
            
            // create a list of sinks with their dependencies    
            let dependencies = inputStages |> List.map buildDependencies             
            
            if List.minBy (snd >> List.length) dependencies |> (snd >> List.length) <> 0 then 
                let pipeStages = inputStages |> List.map snd
                failwith (sprintf "circular dependencies in %A" pipeStages)
                
            if (namedNames |> Set.ofList  |> Set.count) <> (List.length namedNames) then 
                let pipeStages = inputStages |> List.map snd
                let rec findReused names = 
                    match names with 
                    | [] -> failwith "impossible"
                    | hd :: tl -> 
                        if List_contains tl hd then hd else findReused tl
                failwith (sprintf "reused tap name %s" (findReused namedNames))
                    
            for name,_  in tapNames do 
                if (not <| List_contains namedNames name) && (not <| Map.containsKey name taps) then failwith (sprintf "missing tap %s" name)
                                                                                      
            let selectStage (stagesAndSinks:(ISink * PipeStage) list) (acc:(string * ITap option * DispatchStage * ISink) list) =
                let isReady (sink:ISink,stage:PipeStage) = 
                    let deps = dependencies |> List.find (fun (s,dep) -> System.Object.ReferenceEquals (sink,s)) |> snd
                    deps |> List.forall (fun sdep -> acc |> List.exists (fun (_,_,_,s) -> System.Object.ReferenceEquals (sdep,s)))
                let ready,unready = stagesAndSinks |> List.partition isReady 
                List.head ready, (List.tail ready) @ unready                
                                                   
            let createPublishPipe name stagesAndSinks =           
                let stages = stagesAndSinks |> List.map fst            
                BalancedPublishPipe.Create (qm,name,List.length stages)
                                                 
            let rec build (stagesAndSinks:(ISink * PipeStage) list) acc =
                if List.length stagesAndSinks = 0 then acc else            
                let (sink,stage),rest = selectStage stagesAndSinks acc
                let event = new CompletionEvent(false)
                let dsink = DispatchSink (DispatchState.Create (),sink,event)
                let dbuffer = DispatchSingle (DispatchState.Create (),new ThreadBufferPipe(sprintf "sink[%s]" (sink.Name())), dsink)
                let acc' = buildDispatchStage (sink,stage) dbuffer acc
                build rest acc'
                                 
            
            and buildDispatchStage (sink:ISink,pipeStage:PipeStage) dispatchStage acc =
                match pipeStage with 
                | SubscribeStage name -> (name,Map.tryFind name taps,dispatchStage,sink) :: acc
                | NormalStage (ipipe,nextStage) -> 
                    buildDispatchStage (sink,nextStage) (DispatchSingle (DispatchState.Create(),ipipe,dispatchStage)) acc
                | JoinStage (jp,left,right) -> failwith "all join stages should have been removed previously"                
                | PublishStage (name,nextStage) -> 
                    let parentPipes = acc |> List.filter (fun (n,_,_,_) -> n = name) 
                    let parentStages = parentPipes |> List.map (fun (_,_,parent,_) -> parent)
                    if List.isEmpty parentStages then 
                        buildDispatchStage (sink,nextStage) dispatchStage acc
                    else
                        let nextStages = dispatchStage :: parentStages                
                        let publishPipe = createPublishPipe  name ((dispatchStage,sink) :: (parentPipes |> List.map (fun (_,_,parent,psink) -> parent,psink)))
                        let publish = (DispatchPublish (DispatchState.Create(),publishPipe, nextStages))
                        buildDispatchStage (sink,nextStage) publish acc
                    
                    
            let result = build inputStages []
            
            // filter out rows in the result that are from the pseudo-taps -- i.e. the 
            // publish connections.
            let realTapResults = result |> List.filter (fun (_,tapOpt,_,_) -> Option.isSome tapOpt)
            realTapResults |> List.map (fun (n,tapOpt,dstg,_) -> n,Option.get tapOpt,dstg)
                                    
    open PipeImpl                                                                           
        
    /// Collects all of the data into one in-memory sink. Don't use this
    /// on large data sets. (generally for test purposes only) 
    type CollectorSink () = 
        let buffer = ref (Primitives.TupleSequence.Create Schema.Empty Seq.empty)
        interface ISink with            
            member self.Sink (tseq:TupleSequence) =
                buffer := tseq.NewSeq tseq.Schema (tseq |> Seq.cache)
            member self.Name () = (sprintf "%A" self)
        member self.GetSeq = !buffer            

    /// Sink that forces enumeration of the sequences in order, but does not store the result             
    type NullSink (name:string) =         
        interface ISink with            
            member self.Sink tseq =                 
                let enumSeq tseq = for tup in tseq do ()
                enumSeq tseq
            member self.Name () = name
        static member Create (name:string) = new NullSink(name) :> ISink


    /// Given a pipesystem, named taps and sinks, tap the pipes and run them to the sinks 
    let MultiRunPipes (inputStages:(ISink * PipeStage) list) (taps:(string *ITap) list) : unit  =
        let qm = new QueueManager ()
        let dispatchStages = buildDispatchDag qm inputStages (taps |> Map.ofList)
        let ops = 
            [for tapName,tap,stage in dispatchStages do  
                let savedEx = ref None
                let threadFun ()= 
                    try 
                        System.Threading.Thread.CurrentThread.Name <- sprintf "Tap thread: %s" tapName
                        let dispatchCont= buildDispatchPath stage
                        tap.Tap () |> dispatchCont
                    with 
                    | :? SinkDone -> 
                        dispatchException SinkDone stage
                    | ex -> 
                        handleEx ex ("on tap thread " + tapName)
                        savedEx := Some ex 
                        dispatchException ex stage 
                        reraise ()
                yield new System.Threading.Thread (threadFun),savedEx
            ]
        for thread,_ in ops do thread.Start()
        dispatchStages |> List.iter (fun (_,_,stg) -> stg.WaitForSinks ())
        for thread,savedEx in ops do
            thread.Join ()
            match !savedEx with 
            | Some ex -> raise ex // rethrow on main thread
            | None -> ()
        qm.Shutdown ()
                
    /// Given a sink, a pipesystem, and a map of names to taps, tap the pipesystem and run it to the sink 
    let RunPipes (sink:ISink) (pipes:PipeStage) (taps:(string *ITap) list) : unit = MultiRunPipes [(sink,pipes)] taps    
    /// Run a simple pipe, with one tap and one sink and one pipe segment.
    let RunSimplePipe sink pipes (name,tap) =  RunPipes sink pipes [name,tap]

    /// Runs a tuple sequence through single Ipipe.
    let RunPipeFromSeq (tseq:TupleSequence) (ipipe:IPipe) =
        let tap = Tap.FromSeq tseq
        let sink = new CollectorSink ()        
        ipipe.Run tseq (sink :> ISink).Sink
        sink.GetSeq
