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

module internal Cache =
        
      
    let RefCache  (objGen:'a->'b) =
        let caches = 
            Array.init 0x20 (fun _ -> new System.Collections.Generic.Dictionary<'a ,System.WeakReference> ())        
        let cleanupInterval = 0x800
        let cleanupCount = ref cleanupInterval
        
        let inline addToCache ((cache:System.Collections.Generic.Dictionary<'a,System.WeakReference>),(key:'a),(objGen:'a->'b)) : 'b =
                let obj = objGen key
                let ref = new System.WeakReference(obj)
                cache.[key] <- ref
                cleanupCount := !cleanupCount - 1
                if (!cleanupCount <= 0) then
                    cleanupCount := cleanupInterval
                    let cleanupKeys = new System.Collections.Generic.List<'a> ()
                    for kv in cache do
                        if kv.Value.Target = null then cleanupKeys.Add kv.Key
                    for key in cleanupKeys do cache.Remove(key) |> ignore
                obj    
        
            
        let inline cacheFun (key:'a)  : 'b  =
            
            let cacheIdx = ((key :> obj).GetHashCode() |> uint32) % (uint32 caches.Length) |> int32
            let cache = caches.[cacheIdx]
            let entered = ref false

            // .NET FW 4.0: System.Threading.Monitor.TryEnter(cache,entered)
            entered := System.Threading.Monitor.TryEnter(cache)
            if not (!entered) then objGen key else
            try                 
                if cache.ContainsKey(key) then 
                    let cached = cache.[key].Target 
                    if cached <> null then cached :?> 'b else
                        addToCache(cache,key,objGen)                   
                else
                    addToCache(cache,key,objGen)
            finally System.Threading.Monitor.Exit(cache)
        cacheFun
        

    
