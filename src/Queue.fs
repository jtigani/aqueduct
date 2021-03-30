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

module public Queue =                
    type BoundedBlockingQueue<'a> =    
        private {queue:System.Collections.Generic.Queue<'a>
                 itemsAvailable:System.Threading.Semaphore
                 spaceAvailable:System.Threading.Semaphore
                 capacity:int
                 mutable closing:bool
                 mutable closed:bool
                 }
        with
        interface System.IDisposable with
            member self.Dispose() = 
                if self.closed = false then
                    lock self.queue
                        (fun _ ->
                            self.itemsAvailable.Close()
                            self.spaceAvailable.Close()
                            self.closed <- true)
                    
        static member Create(size:int) =         
            if (size <= 0) then raise <| System.ArgumentOutOfRangeException ("size")
            {   itemsAvailable = new System.Threading.Semaphore(0, size)
                spaceAvailable = new System.Threading.Semaphore(size, size)
                closing = false
                closed = false
                capacity = size
                queue = new System.Collections.Generic.Queue<'a>(size)}

        member self.Count = lock (self.queue) (fun _ -> self.queue.Count)
        member self.Capacity =  self.capacity
            
        member self.Enqueue (data) =
            if self.closed || self.closing then raise <| System.ArgumentException ("trying to add to a closing or closed queue")
            self.spaceAvailable.WaitOne() |> ignore            
            lock (self.queue) (fun _ -> self.queue.Enqueue(data)
                                        self.itemsAvailable.Release() |> ignore)
            
        
        member self.Dequeue () : 'a =            
            if self.closed then raise <| System.ArgumentException ("trying to read from a closed queue")
            self.itemsAvailable.WaitOne() |> ignore
            self.DequeueNoWait ()
        
        member self.Drain() : 'a list =            
            if self.closed then raise <| System.ArgumentException ("trying to read from a closed queue")
            lock (self.queue) (fun _ ->
                                [ while self.itemsAvailable.WaitOne(0) do yield self.DequeueNoWait () ])
                                                
            
        member private self.DequeueNoWait () =
            let lockedDequeue _ = 
                let item = self.queue.Dequeue()
                self.spaceAvailable.Release() |> ignore
                item
                
            let item = lock (self.queue) lockedDequeue
            
            if self.closing && self.IsEmpty () then (self :> System.IDisposable).Dispose ()
            item
            
        static member DequeueAny (queues:BoundedBlockingQueue<'a> array) : (int * 'a) = 
            let mappings = [| for ii in 0 .. (queues.Length - 1) do if queues.[ii].IsClosed() then () else yield (ii,queues.[ii]) |]
            let ids,active = Array.unzip mappings            
            let arr:System.Threading.WaitHandle array = 
                Array.init (Array.length active) (fun i -> active.[i].itemsAvailable :> System.Threading.WaitHandle)
            let idx = System.Threading.WaitHandle.WaitAny arr
            ids.[idx],active.[idx].DequeueNoWait ()
                                    
        static member ProbeAny (queues:BoundedBlockingQueue<'a> array) : (int * 'a) = 
            let mappings = [| for ii in 0 .. (queues.Length - 1) do if queues.[ii].IsClosed() then () else yield (ii,queues.[ii]) |]
            let ids,active = Array.unzip mappings                        
            let arr:System.Threading.WaitHandle array = 
                Array.init (Array.length active) (fun i -> active.[i].itemsAvailable :> System.Threading.WaitHandle)
            let idx = System.Threading.WaitHandle.WaitAny arr
            ids.[idx],active.[idx].ProbeNoWait ()
                                                                                                                        
        member self.IsEmpty () : bool = lock self.queue (fun _ -> self.queue.Count = 0)
        member self.IsClosed() : bool = self.closed
        member self.BeginClose () : unit =
            lock (self.queue) (fun _ ->
                self.closing <- true
                if self.IsEmpty () then (self :> System.IDisposable).Dispose ()
            )
                
        member self.Replace (f:'a seq->'a seq) : unit =
            // if the queue is trying to close down, we don't know if it will
            // be closed under us, so we should tell it to stop closing            
            let acquireSemMultiple (sem:System.Threading.Semaphore) = 
                seq [while sem.WaitOne (0) do yield ()] |> Seq.length
            
            
            let runReplaceLocked _ = 
                if self.closed then () else            
                // acquire both the space available and the items available semaphores
                // so that nobody can use the queue                            
                let ownedSpaces = acquireSemMultiple self.spaceAvailable            
                let ownedItems = acquireSemMultiple self.itemsAvailable
                let drained = [while self.queue.Count <> 0 do yield self.queue.Dequeue ()]
                let restored = f drained |> List.ofSeq
                let listDelta = (List.length drained) - (List.length restored)
                if listDelta < 0 then failwith "cannot replace with more items than were drained"
                for item in restored do 
                    self.queue.Enqueue item 
                    
                let itemCount = ownedItems - listDelta
                if itemCount < 0 then failwith "used queue slots we didn't have!"
                else if itemCount > 0 then
                    self.itemsAvailable.Release (itemCount) |> ignore
                
                let spaceCount = ownedSpaces + listDelta                
                if spaceCount < 0 then failwith "used queue slots we didn't have!"
                
                else if spaceCount > 0 then
                    self.spaceAvailable.Release (spaceCount) |> ignore
                
                                                                    
            lock (self.queue) runReplaceLocked
                        
                                                            
        member self.Probe () : 'a =
            self.itemsAvailable.WaitOne() |> ignore            
            self.ProbeNoWait ()
        
        member private self.ProbeNoWait () : 'a =
            lock (self.queue) (fun _ ->self.queue.Peek())
            
        member self.Release () : unit =
            let lockedRelease _ = 
                let item = self.queue.Dequeue() |> ignore
                self.spaceAvailable.Release() |> ignore                
            lock (self.queue) lockedRelease
            
            
    and BlockingPriorityQueue<'a> = 
        private {queues:BoundedBlockingQueue<'a> array}
        with                            
        static member Create(count:int,size:int) =         
            if (size <= 0) then raise <| System.ArgumentOutOfRangeException ("size")
            if (count<= 0) then raise <| System.ArgumentOutOfRangeException ("count")
            {   queues = Array.init count (fun _ -> BoundedBlockingQueue<'a>.Create(size)) }

        member self.Enqueue (pri:int,data:'a) : unit = self.queues.[pri].Enqueue(data)
        
        member self.DequeueAny () : int * 'a = BoundedBlockingQueue.DequeueAny (self.queues)
        member self.Dequeue (pri:int) : 'a = self.queues.[pri].Dequeue ()
                        
        member self.IsEmpty () : bool = self.queues |> Array.forall (fun (q:BoundedBlockingQueue<_>) -> q.IsEmpty ())
        member self.AreAllClosed() : bool = self.queues |> Array.forall (fun (q:BoundedBlockingQueue<_>) -> q.IsClosed())
        member self.IsClosed(idx) : bool = self.queues.[idx].IsClosed()
        member self.BeginClose idx : unit = self.queues.[idx].BeginClose()
                                                    
    

