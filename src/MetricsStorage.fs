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

namespace Aqueduct

/// Module containing types allowing pipes to be serialized to storage.
module public MetricsStorage =
    open Aqueduct.Primitives    
    
    exception StorageFailure of string 
    
    /// Common interface for storing pipe streams.
    type IMetricsStorage =
        abstract Close: unit -> unit
        abstract AddSeq: (Primitives.Tuple->Primitives.Datum list)->Primitives.TupleSequence->unit
        abstract Find: Primitives.Datum list->Primitives.TupleSequence
        abstract ReadSchema: unit -> Primitives.Schema
        abstract HasData: unit -> bool
        abstract Read: unit -> Primitives.TupleSequence
        abstract ReadKeysAndValues: unit -> (Datum list * Tuple) seq
        abstract JoinInner: IMetricsStorage->(Primitives.Tuple->Primitives.Tuple->Primitives.Tuple)->Primitives.Tuple seq
        abstract JoinLeftOuter: IMetricsStorage->(Primitives.Tuple->Primitives.Tuple->Primitives.Tuple)->Primitives.Tuple seq
        

    let private up a = a :> IMetricsStorage
         
    /// Metrics storage that is backed by memory. Only use when you know the stream will be small
    /// enought to fit into memory.   
    type MetricsStorageMemory =                         
        val private storage:System.Collections.Generic.SortedList<Datum list , System.Collections.Generic.List<Tuple>>
        val mutable private schema:Schema option
    with      
        new () = {schema=None;storage=new System.Collections.Generic.SortedList<Datum list , System.Collections.Generic.List<Tuple>>()}
        
        interface IMetricsStorage with
            member self.Close () = ()
                                    
            member x.AddSeq (keygen:(Primitives.Tuple -> Primitives.Datum list)) (tuples:Primitives.TupleSequence) =            
                if x.schema = None then x.schema <- Some tuples.Schema
                for tuple in tuples.tseq do
                    let key = keygen tuple
                    if not <| x.storage.ContainsKey key then  x.storage.[key] <- new System.Collections.Generic.List<Tuple>()
                    x.storage.[key].Add(tuple)                        
                                                                                        
            member x.Find (key:Primitives.Datum list) : Primitives.TupleSequence=
                if x.storage.Count = 0  || not <| x.storage.ContainsKey key then 
                    raise <| StorageFailure (sprintf "Unable to find key %A" key)
                else
                    let seq  = x.storage.[key] :> Tuple seq
                    Primitives.TupleSequence.CreateUnsafe ((up x).ReadSchema()) seq
                
            member x.ReadSchema() : Primitives.Schema = 
                match x.schema with
                | None -> raise <| StorageFailure "Unable to read schema: No sequences seen" 
                | Some schema -> schema
                
            member x.HasData () = x.storage.Count <> 0 
            member x.ReadKeysAndValues () = 
                seq {
                    for kv in x.storage do
                        for v in kv.Value do
                            yield kv.Key,v
                        
                }
            member x.Read() : Primitives.TupleSequence =            
                let schema = ((up x).ReadSchema())
                let seq  = x.storage.Values |> Seq.concat |> Seq.cache
                Primitives.TupleSequence.CreateUnsafe schema seq                                                                                                    
                                    
            member x.JoinInner 
                (that:IMetricsStorage) (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple) : Primitives.Tuple seq =
                
                seq {
                    for k,v in that.ReadKeysAndValues() do                        
                        if x.storage.ContainsKey k then
                            let rightTup = x.storage.[k].[0]
                            yield combineTuples v rightTup
                }
                
            member x.JoinLeftOuter 
                (that:IMetricsStorage) (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple): Primitives.Tuple seq =
                let defaultRight = ((up x).ReadSchema ()).DefaultTuple
                seq {
                    for k,v in that.ReadKeysAndValues() do                        
                        if x.storage.ContainsKey k then
                            let rightTup = x.storage.[k].[0]
                            yield combineTuples v rightTup
                        else 
                            yield combineTuples v defaultRight
                }                                                                
        static member Create () = new MetricsStorageMemory() |> up            
          
    /// 'Fake' metrics storage -- drops all tuples on the floor.              
    type MetricsStorageNone =                         
        new () = {}
    with                      
        interface IMetricsStorage with
            member self.Close () = ()
                                    
            member x.AddSeq (keygen:(Primitives.Tuple -> Primitives.Datum list)) (tuples:Primitives.TupleSequence) =            
                failwith "not a real storage table."
                                                                                        
            member x.Find (key:Primitives.Datum list) : Primitives.TupleSequence=
                failwith "not a real storage table."
                
            member x.ReadSchema() : Primitives.Schema = failwith "not a real storage table."
                
            member x.HasData () = false
            member x.ReadKeysAndValues () = failwith "not a real storage table."
            member x.Read() : Primitives.TupleSequence = failwith "not a real storage table."
                
                                    
            member x.JoinInner 
                (that:IMetricsStorage) (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple) : Primitives.Tuple seq =
                failwith "not a real storage table."

            member x.JoinLeftOuter 
                (that:IMetricsStorage) (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple): Primitives.Tuple seq =
                failwith "not a real storage table."                                    
        static member Create () = new MetricsStorageNone() |> up

    /// Take a tuple sequence and buffer it to file. The resulting sequence
    /// will be backed by a temporary file.
    let BufferTseq (tseq:TupleSequence) : TupleSequence =       
        let fileName = System.IO.Path.Combine(System.IO.Path.GetTempPath (), "metrics" + (System.Guid.NewGuid ()).ToString ()) + ".tmp"
        use writeStream = new System.IO.FileStream (fileName,System.IO.FileMode.CreateNew)        
        Serialization.serializeTupleSequence Serialization.BinaryFormat writeStream  tseq    
        writeStream.Flush ()
        writeStream.Close ()
        let bufferedSeq  = seq {
            use readStream = new System.IO.FileStream (fileName,System.IO.FileMode.Open)
            let buffered = Serialization.deserializeTupleSequence Serialization.BinaryFormat readStream
            yield! buffered.tseq
        }
        TupleSequence.CreateUnsafe tseq.schema bufferedSeq
      
    /// MetricsStorage backed by multiple metrics storages. Tuples 
    /// are assigned to storage by hashing based on their keys. 
    /// For database backed storage, can improve performance to read to / write from
    /// multiple tables at once.
    type MultiStorage = 
        val private tables:IMetricsStorage array        
    with      
        new (tables) = {tables=tables}
        static member Create (fanOut, tableGen:unit->IMetricsStorage) =                   
            let tables = Array.init fanOut (fun _ -> tableGen ())
            new MultiStorage (tables) |> up
        
        member private x.Release sch (lst:System.Collections.Generic.List<Tuple>) (keygen:Tuple->Datum list) =
            let tuples = lst.ToArray () |> Seq.ofArray
            let seqs = 
                [|for ii in 0..(x.tables.Length - 1) do
                        let filtered = tuples |> Seq.filter (fun (tup) -> x.Hash (keygen tup) = ii)
                        yield TupleSequence.CreateUnsafe sch filtered
                |]
            lst.Clear ()
            let asyncs = [for ii in 0..(x.tables.Length - 1) do 
                                yield async {x.tables.[ii].AddSeq keygen seqs.[ii]}]
            Async.Parallel asyncs |> Async.RunSynchronously |> ignore
            
        member private x.Hash (dtm:Datum list) = 
            (dtm.GetHashCode () |> uint32) % (x.tables.Length |> uint32) |> int
                
        interface IMetricsStorage with
            member self.Close () = self.tables |> Array.iter (fun tab -> tab.Close ())
            member x.AddSeq (keygen:(Primitives.Tuple -> Primitives.Datum list)) (tuples:Primitives.TupleSequence) =
                let lst = new System.Collections.Generic.List<Tuple>(0x1000)
                for tup in tuples.tseq do
                    lst.Add tup
                    if lst.Count = 0x1000 then
                        x.Release tuples.Schema lst keygen
                x.Release tuples.Schema lst keygen
                                                                
            member x.Find (key:Primitives.Datum list) : Primitives.TupleSequence=
                let idx = x.Hash key
                x.tables.[idx].Find key
                
            member x.ReadSchema() : Primitives.Schema =
                x.tables.[0].ReadSchema ()
                
                
            member x.HasData () = 
                x.tables |> Seq.tryFind (fun tab -> tab.HasData ()) <> None
                
            member x.ReadKeysAndValues () : (Primitives.Datum list * Primitives.Tuple) seq =            
                
                let tseq =
                    seq {
                        let seqs = x.tables |> Array.map (fun tab -> tab.ReadKeysAndValues ())
                        let iterList = 
                            let allIters  = 
                                    seqs |> 
                                    Array.map (fun kvseq -> kvseq.GetEnumerator ()) 
                            let asyncs = 
                                allIters |> Array.map (fun it -> async {let hasNext = it.MoveNext () = true 
                                                                        if hasNext = false then it.Dispose ()
                                                                        return hasNext})
                                                                        
                            let haveNext = Async.Parallel (asyncs) |> Async.RunSynchronously
                            let iters = [| for ii in 0..(allIters.Length-1) do if haveNext.[ii] then yield allIters.[ii] |]
                            
                            new System.Collections.Generic.List<_>(iters)
                                                                    
                        while (iterList.Count <> 0) do                         
                            let nextIter = iterList |> Seq.minBy (fun iter -> iter.Current |> fst)
                            yield nextIter.Current
                            if nextIter .MoveNext () = false then
                                let idx = iterList |> Seq.findIndex (fun iter -> System.Object.ReferenceEquals(nextIter,iter))
                                iterList.RemoveAt(idx)
                                nextIter.Dispose ()
                    }
                tseq   

            member x.Read() : Primitives.TupleSequence =                                
                let schema = (x |> up).ReadSchema ()    
                let tseq = 
                    (x |> up).ReadKeysAndValues () |> Seq.map snd                
                Primitives.TupleSequence.CreateUnsafe schema tseq                 
                                                    
            member x.JoinInner 
                (that:IMetricsStorage) 
                (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple) : Primitives.Tuple seq =
                failwith "not supported"
                
            member x.JoinLeftOuter 
                (that:IMetricsStorage) 
                (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple) : Primitives.Tuple seq =                
                failwith "not supported"