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

namespace Aqueduct.Internal

module internal QueueManager =
    open Aqueduct
    open Aqueduct.Primitives
    open Aqueduct.Internal.Queue
    
        
    /// A queue will get passed One or more InputCmds containing typed sequences of tuples
    ///    and then DoneCmd signaling there will be no more input
    type internal PipeCmd = InputCmd of TupleSequence | DoneCmd

    type private SequencePointer =
        | SequenceNormal of TupleSequence * int
        | SequenceFile of TupleSequence * int
        | SequenceEnd
        
    type private QueueEntry = {sequence:SequencePointer}
                  
    type private InternalQueue =BoundedBlockingQueue<QueueEntry>
       
    type ManagedQueue = 
        private {enq:PipeCmd->unit;deq:unit->PipeCmd;q:InternalQueue}
        with
            static member Create(enq,deq,q) = {enq=enq;deq=deq;q=q}
            member x.Enqueue y = x.enq y
            member x.Dequeue () = x.deq ()
            member x.InternalQueue  = x.q
        
    type QueueGroup = 
        private {enq:PipeCmd->unit;deq:int->PipeCmd;qs:InternalQueue array}
        with
            static member Create(enq,deq,qs) : QueueGroup = {enq=enq;deq=deq;qs=qs} 
            member x.Enqueue y = x.enq y
            member x.Dequeue idx = x.deq idx
            member x.InternalQueues  = x.qs
        
    type QueueList = System.Collections.Generic.List<InternalQueue array*string>
    type QueueManager() =
        let activeQueues = new QueueList ()
        let wakeHandle = new System.Threading.ManualResetEvent (false)
        let shutdown = ref false
        
        let queueUpdateNormal (entries:QueueEntry seq) =
            // replace sequence backed with table backed sequences
            let rec mergeNormal (sch,seqs,len) sequences acc =
                match sequences with
                | SequenceNormal (tseq,len1) :: tl when len < 0x5 ->mergeNormal (sch,tseq :: seqs,len+len1) tl acc
                | _  ->
                    let combined = seqs |> List.rev |> Seq.concat
                    let cached = combined |> TupleSequence.CreateUnsafe sch  |> TupleTable.CacheTseq
                    merge sequences (SequenceNormal(cached,len) :: acc)
            and merge sequences acc =
                match sequences with 
                | [] -> acc 
                | SequenceEnd :: tl -> merge tl (SequenceEnd :: acc)
                | SequenceNormal (seq1,len1):: tl  -> mergeNormal (seq1.Schema,[seq1],len1) tl acc
                | hd :: tl -> merge tl (hd :: acc)
            let entries = entries |> List.ofSeq
            if List.length entries = 0 then Seq.empty else
            let sequences = entries |> List.map (fun (entry:QueueEntry) -> entry.sequence)
            let newSequences = merge sequences [] |> List.rev           
            seq [for sequence in newSequences do yield {sequence=sequence}]
                            
        let spillLimit = 0x10
        let queueUpdatePressure (entries:QueueEntry seq) =
            let rec mergeNormal (sch,seqs,len) sequences acc =
                match sequences with
                | SequenceNormal (tseq,len1) :: tl when len < 0x5 ->mergeNormal (sch,tseq :: seqs,len+len1) tl acc                                
                | _  ->
                    let combined = seqs |> List.rev |> Seq.concat
                    let cacheFun = if len <= 0x5 then TupleTable.CacheTseq else MetricsStorage.BufferTseq
                    let cached = combined |> TupleSequence.CreateUnsafe sch  |> cacheFun
                    merge sequences (SequenceFile(cached,len) :: acc)
            and mergeFile (sch,seqs,len) sequences acc =
                match sequences with
                | SequenceFile (tseq,len1) :: tl -> mergeFile (sch,tseq :: seqs,len+len1) tl acc
                | _ ->
                    let combined = seqs |> List.rev |> Seq.concat |> TupleSequence.CreateUnsafe sch
                    merge sequences (SequenceFile(combined,len) :: acc)                
            and merge sequences acc =
                match sequences with 
                | [] -> acc                
                | SequenceEnd :: tl -> merge tl (SequenceEnd :: acc)
                | SequenceNormal (seq1,len1):: tl  -> mergeNormal (seq1.Schema,[seq1],len1) tl acc
                | SequenceFile (seq1,len1) :: tl  -> mergeFile (seq1.Schema,[seq1],len1) tl acc                

            let entries = entries |> List.ofSeq
            if List.length entries = 0 then Seq.empty else
            let sequences = entries |> List.map (fun (entry:QueueEntry) -> entry.sequence)
            let newSequences = merge sequences [] |> List.rev
            
            seq [for sequence in newSequences do yield {sequence=sequence}]                    
        
        let rec threadFun () =            
            // wake up to do queue maintenance every 10 seconds or when we're asked to.
            if wakeHandle.WaitOne (10 * 1000) then
                wakeHandle.Reset () |> ignore
            if !shutdown then () else     
            let activeQueuesCapture = lock (activeQueues) (fun _ -> activeQueues |> List.ofSeq)
            let compressQueues = 
                [for qs,n in activeQueuesCapture do 
                    if Seq.length qs <> 0 then 
                        // when at least one queue in a queue group is full, and at least one is less than half full, we've 
                        // got an unbalanced block, so we should try to complress the queue
                        let critical = 
                                qs.Length > 1 && (qs |> Seq.minBy (fun q -> q.Count)).Count * 2 < qs.[0].Capacity &&
                                (qs |> Seq.maxBy (fun q -> q.Count)).Count = qs.[0].Capacity
                        let ii = ref 0
                        for q in qs do
                            ii := !ii + 1
                            if (2 * q.Count) > q.Capacity  then yield q,critical,n,!ii,q.Count]
                
            // if all queues are either blocked or empty, start compressing queues and
            // spilling them to disk if necessary.
                                
            for q,critical,n,idx,countCapture in compressQueues do                
                if critical && (q.Count = q.Capacity) then                                                         
                    q.Replace queueUpdatePressure
                    fprintfn System.Console.Error "Compressed queue %s.%d (%d/%d)" n idx q.Count q.Capacity
                else if q.Count = countCapture then                    
                    q.Replace queueUpdateNormal
                    //fprintfn System.Console.Error "Organized queue %s. (%d/%d)" n q.Count q.Capacity
                else ()
                                
            if !shutdown then () else
            threadFun ()

        let queueThread = new System.Threading.Thread (threadFun)
        
        let addQueue q =lock (activeQueues) (fun _ -> activeQueues.Add q)
        let removeClosedQueues ()=
            lock (activeQueues) 
                    (fun _ -> 
                        let newActive = activeQueues |> Seq.filter (fun (qs,n) -> not (qs |> Array.forall (fun q -> q.IsClosed()))) |> List.ofSeq
                        if List.length newActive <> Seq.length activeQueues then
                            activeQueues.Clear ()
                            activeQueues.AddRange(newActive))                        
        with
        member x.NewQueue (capacity:int) (name:string) =
            if queueThread.ThreadState = System.Threading.ThreadState.Unstarted then
                lock queueThread (fun _ -> if queueThread.ThreadState = System.Threading.ThreadState.Unstarted  then queueThread.Start ())
            let q = InternalQueue.Create (capacity)
            let qs = Array.create 1 q
            addQueue (qs,name)            
            ManagedQueue.Create (x.Enqueue q, x.Dequeue q,q)            
            
        member x.NewQueueGroup (count:int) (capacity:int) (name:string) =
            if count = 0 then
                QueueGroup.Create ((fun _ -> ()), (fun _ -> failwith "unexpected dequeue"), Array.empty)
            else
            if queueThread.ThreadState = System.Threading.ThreadState.Unstarted then
                lock queueThread (fun _ -> if queueThread.ThreadState = System.Threading.ThreadState.Unstarted  then queueThread.Start ())
                
            let qs = Array.init count (fun _ -> InternalQueue.Create (capacity))
                                    
            addQueue (qs,name)
            let enq cmd = x.EnqueueMulti qs cmd
            let deq idx = x.Dequeue qs.[idx] ()
            QueueGroup.Create (enq, deq,qs)            
            
            
        member private x.Enqueue (q:InternalQueue) (cmd:PipeCmd) =
            let ptr = 
                match cmd with 
                | InputCmd tseq -> SequenceNormal (tseq,1)
                | DoneCmd -> SequenceEnd
            let entry = {sequence=ptr}
            //if q.Count = q.Capacity then wakeHandle.Set () |> ignore            
            q.Enqueue entry
            match cmd with 
                | InputCmd tseq -> ()
                | DoneCmd -> q.BeginClose ()
            
        member x.EnqueueMulti (qs:InternalQueue seq) (cmd:PipeCmd) =
            let ptr = 
                match cmd with 
                | InputCmd tseq -> SequenceNormal (tseq,1)
                | DoneCmd -> SequenceEnd
            let entries = [for q in qs do yield {sequence=ptr}]
              
            // if we can't insert because some queues are full, compress the full queues            
            let rec enqueueAll (qlist:(InternalQueue) list) = 
                let full,notFull = qlist |> List.partition (fun q -> q.Count = q.Capacity)            
                for q in notFull do x.Enqueue q cmd
                match full with
                | q:: tl -> x.Enqueue q cmd; enqueueAll tl
                | [] -> ()
            qs |> List.ofSeq |> enqueueAll 
                                    
        member x.Dequeue (q:InternalQueue) () : PipeCmd =
            let entry = q.Dequeue () 
            let result = 
                match entry.sequence with
                | SequenceNormal (tseq,_)
                | SequenceFile (tseq,_) -> InputCmd tseq
                | SequenceEnd -> DoneCmd
            
            if q.IsClosed () then removeClosedQueues ()
            result
            
        member x.Shutdown () = 
            shutdown := true
            wakeHandle.Set () |> ignore
            if queueThread.ThreadState <> System.Threading.ThreadState.Unstarted then
                queueThread.Join ()
