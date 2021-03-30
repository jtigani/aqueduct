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

/// Module containing storage information for database-backed metirics storage
module public MetricsDb =
    open Aqueduct.Internal

    /// Exception raised when there is a daabase exception.
    exception StorageFailure of string 
    
    let private metricsDb = ref None
    
    /// Globally set the database used as temporary metrics spill storage for sorts and joins.
    let public SetMetricsDb (db:Database.DbData)  = 
        metricsDb := Some db
    
    let internal Db () = 
        match !metricsDb with
        | Some db -> db
        | None -> failwith "Must set metrics db before using"
        
    
    let private addParam (cmd : System.Data.Common.DbCommand) (name:string) (x : obj) =
      let param = cmd.CreateParameter()
      param.ParameterName <- (Db ()).paramName name      
      param.Value <- x      
      cmd.Parameters.Add(param) |> ignore      
    
    let private addReusableParam (cmd : System.Data.Common.DbCommand) (name:string)  =
      let param = cmd.CreateParameter()
      param.ParameterName <- (Db ()).paramName name            
      cmd.Parameters.Add(param) |> ignore      
    
    let private assignParam (cmd : System.Data.Common.DbCommand) (name:string) (x:obj) =
        let param = cmd.Parameters.[(Db ()).paramName name]
        param.Value <- x
        
    type  internal StorageInfo = { 
        keyTypes:Primitives.SchemaElement list
        mutable connection: System.Data.Common.DbConnection option        
        mutable sortedTableBuilt:bool
        initTid:int
    }
    
    let private up a = a :> MetricsStorage.IMetricsStorage
                   
    /// MetricsStorage backed by a database. 
    type MetricsStorageTable=                 
        val private name:string
        val mutable private storage:StorageInfo option       
    with      
        new (name:string) = {name=name;storage=None}
        
        member private x.makeTempTableStr = 
            let builder = new System.Text.StringBuilder()            
            let idxStr = System.String.Join(",", x.getKeyFields |> Array.ofList)
            let len = x.name.Length
            builder.Append(sprintf "CREATE TABLE IF NOT EXISTS MetricsSchemas (id CHAR(%d)  NOT NULL,data BLOB NOT NULL);" len).
                    Append(sprintf "DROP TABLE IF EXISTS %s;" x.name).                    
                    Append(sprintf "CREATE TABLE %s (%s,data BLOB NOT NULL);" x.name x.keyCreateStr) |> ignore                    
                    
            builder.ToString() |> (Db()).hackDbCreate
            
        member private x.keyCreateStr = 
            let rec keystr keyType =                 
                match keyType with
                | [] -> []
                | Primitives.IntElement :: tl -> "BIGINT(20)" :: keystr tl
                | Primitives.UnsignedElement :: tl -> "BIGINT(20) UNSIGNED" :: keystr tl
                | Primitives.FloatElement :: tl -> "REAL" :: keystr tl
                | Primitives.StringElement :: tl -> "CHAR(32)" :: keystr tl
                | _ -> failwith "keytype not supported"
                
            let keyStrs = x.keyTypes |> keystr 
            let keyCreateStrs = List.zip  x.getKeyFields keyStrs |> List.map (fun (field, key) -> sprintf "%s %s NOT NULL" field key)
            System.String.Join (",",Array.ofList keyCreateStrs)
            
        member private x.makeSortedTableStr = 
            let builder = new System.Text.StringBuilder()            
            builder.Append(sprintf "DROP TABLE IF EXISTS %s;" x.sortedName).                    
                    Append(sprintf "CREATE TABLE %s (rowid integer PRIMARY KEY AUTO_INCREMENT,%s,data BLOB NOT NULL);" x.sortedName x.keyCreateStr) |> ignore
            builder.ToString() |> (Db()).hackDbCreate
            
            
        member private x.connection = 
            match x.storage.Value.connection with
            | Some connection -> connection
            | None ->             
                let connection = (Db()).Connect()
                x.storage.Value.connection <- Some connection
                connection
            
        member private x.keyTypes = x.storage.Value.keyTypes
        
        member private x.makeTempTable () =
            use command = x.connection.CreateCommand()            
            command.CommandText <- x.makeTempTableStr 
            command.ExecuteNonQuery () |> ignore
            ()
            
        member private x.sortedName = x.name + "_sorted"
                
        member private x.makeKey (key:Primitives.Datum) =
            match key.Typeof with 
            | Primitives.IntElement -> box (Primitives.Datum.ExtractInt key)
            | Primitives.UnsignedElement -> box (Primitives.Datum.ExtractUnsigned key)
            | Primitives.FloatElement -> box (Primitives.Datum.ExtractFloat key)
            | Primitives.StringElement  -> let s = (Primitives.Datum.ExtractString key)
                                           (if (s.Length > 32) then s.Substring(0,32) else s) :> obj // only use the first 32 chars to compare
            | Primitives.SequenceElement _ -> failwith "tuples cannot be a key"
                                
        member private x.getKeyData (reader: System.Data.Common.DbDataReader) =
            let getKeyDatum index keyType =
                match keyType with                                
                | Primitives.IntElement -> reader.GetValue index |> Database.dbIntToInt64 |> Primitives.Datum.Int 
                | Primitives.UnsignedElement -> reader.GetValue index |> Database.dbUintToUint64 |> Primitives.Datum.Unsigned 
                | Primitives.FloatElement -> reader.GetDouble index |> Primitives.Datum.Float 
                | Primitives.StringElement-> reader.GetString index |> Primitives.Datum.String 
                | Primitives.SequenceElement _ -> failwith "keytype not supported"
            x.keyTypes |> List.mapi getKeyDatum 
        
        member private x.getKeyFields = x.keyTypes |> List.mapi (fun i _ -> "id_" + string i) 
            
        member private x.makeInsertSchemaCommand (cmd:System.Data.Common.DbCommand) (schema:Primitives.Schema) =
            let writer = new System.IO.MemoryStream()            
            let formatter = Serialization.BinaryFormat
            schema |> Serialization.serializeSchema formatter writer            
            let schStr = writer.GetBuffer ()
            let keyobj = x.name
            cmd.CommandText <- sprintf "INSERT INTO MetricsSchemas VALUES (%s,%s)" ((Db ()).paramText "key") ((Db ()).paramText "value")                 
            addParam cmd "key" keyobj
            addParam cmd "value" schStr            
            ()
        
        member private x.assignMultiInsertCommand 
            (cmd:System.Data.Common.DbCommand) (keygen:Primitives.Tuple->Primitives.Datum list) (tuples:Primitives.Tuple seq) =
            tuples |> Seq.iteri (fun idx tup -> x.assignInsertCommand cmd (keygen tup) tup (idx + 1))
            
        member private x.assignInsertCommand (cmd:System.Data.Common.DbCommand) (keys:Primitives.Datum list) (tuple:Primitives.Tuple) (idx:int) =
            let defstr = Serialization.compressTuple tuple
            if defstr.Length > 1000 * 1000 then 
                failwith "packet too large"
                
            keys |> List.iter2 (fun kname key -> 
                                let keyobj = x.makeKey key                
                                assignParam cmd (kname  + string idx) keyobj) x.getKeyFields
            assignParam cmd ("value" + string idx) defstr
            
        member private x.makeInsertCommand (cmd:System.Data.Common.DbCommand) (cnt:int) =
                                        
            let builder = new System.Text.StringBuilder ()
            let insertOne (n:int) =
                let keys = [| for k in x.getKeyFields do 
                                addReusableParam cmd (k + string n)
                                yield (Db ()).paramText (k + string n)|]
                let keyString:string = System.String.Join (",", keys)
                let valueString:string = (Db ()).paramText ("value" + string n)
                addReusableParam cmd ("value" + string n)
                
                builder.Append ("(") |> ignore
                builder.Append (keyString) |> ignore
                builder.Append (",") |> ignore
                builder.Append (valueString) |> ignore
                builder.Append (")") |> ignore

            let anchor:string = sprintf "INSERT INTO %s VALUES " x.name
            builder.Append (anchor) |> ignore
            
            for ii in 1..cnt do 
                insertOne ii
                if ii < cnt then builder.Append (",") |> ignore
            cmd.CommandText <- builder.ToString ()
            cmd.Prepare ()
            ()
            
        member private x.makeFindCommand (keys:Primitives.Datum list) =
            let cmd:System.Data.Common.DbCommand = x.createCommand ()
            let kstrs =  
                x.getKeyFields  |>
                List.map (fun kname -> sprintf "%s=%s" kname ((Db ()).paramText kname)) |> 
                Array.ofList
            cmd.CommandText <- sprintf "SELECT %s FROM %s WHERE %s" "data" x.name (System.String.Join(" and ", kstrs))
            List.iter2 (fun kname key -> addParam cmd kname (x.makeKey key)) x.getKeyFields keys
            cmd
            
        member private x.makeFindSchemaCommand () =
            let cmd:System.Data.Common.DbCommand = x.createCommand ()
            let keyobj = x.name
            cmd.CommandText <- sprintf "SELECT data FROM MetricsSchemas WHERE id=%s" ((Db ()).paramText "key")
            addParam cmd "key" keyobj            
            cmd
            
        member private x.makeReadCommand  () =            
            let cmd:System.Data.Common.DbCommand = x.createCommand ()
            cmd.CommandTimeout <- 10000
            let keyfields = System.String.Join (",", x.getKeyFields |> Array.ofList)
            cmd.CommandText <- sprintf "SELECT %s,%s FROM %s WHERE rowid > %s ORDER BY rowid LIMIT %s" keyfields "data" x.sortedName  ((Db ()).paramText "start") ((Db ()).paramText "limit")
            addReusableParam cmd "start" 
            addReusableParam cmd "limit"
            cmd
            
                        
        member private x.buildSortedTableCommand () =
            let cmd:System.Data.Common.DbCommand = x.createCommand ()            
            let builder = new System.Text.StringBuilder ()
            let keyfields = System.String.Join (",", x.getKeyFields |> Array.ofList)
            builder.Append(x.makeSortedTableStr).
                    Append(sprintf "insert into %s (%s,data) SELECT %s,data from %s order by %s" x.sortedName keyfields keyfields x.name keyfields) |> ignore
            cmd.CommandText <- builder.ToString ()
            cmd.CommandTimeout <- 100 * 1000
            cmd
            
        member private x.makeInnerJoinCommand (other:MetricsStorageTable) =
            let cmd:System.Data.Common.DbCommand = x.createCommand ()
            cmd.CommandTimeout <- 10000
            let where =
                let subwheres = List.map2 (fun kl kr -> sprintf "%s.%s = %s.%s" x.name kl other.name kr) x.getKeyFields other.getKeyFields 
                System.String.Join (" and ", subwheres |> Array.ofList)
            cmd.CommandText 
                <- sprintf "SELECT %s.data,%s.data FROM %s, %s WHERE %s" 
                   x.name other.name
                   x.name other.name where
            cmd
            
        member private x.makeLeftOuterJoinCommand (other:MetricsStorageTable) =            
            let cmd:System.Data.Common.DbCommand = x.createCommand ()
            cmd.CommandTimeout <- 10000
            let where =
                let subwheres = List.map2 (fun kl kr -> sprintf "%s.%s = %s.%s" x.name kl other.name kr) x.getKeyFields other.getKeyFields 
                System.String.Join (" and ", subwheres |> Array.ofList)
            cmd.CommandText 
                <- sprintf "SELECT %s.data,%s.data FROM %s LEFT OUTER JOIN %s ON %s" 
                    x.name other.name
                    x.name other.name where
            cmd
                                
        static member private md5Hash (str:string) = 
             // Create a new instance of the MD5CryptoServiceProvider object.
            let hasher =    System.Security.Cryptography.MD5.Create()
            let keyBytes:byte array = System.Text.Encoding.Default.GetBytes(str)
            // Convert the input string to a byte array and compute the hash.
            let data = hasher.ComputeHash(keyBytes)
            System.Convert.ToBase64String(data)

        member private x.isInitialized = x.storage <> None
                
                                  
        member public x.AddSchema (sch:Primitives.Schema) =
            use command = x.createCommand ()                        
            x.makeInsertSchemaCommand command sch
            command.ExecuteNonQuery () |> ignore            
            
        member private x.Init (keyType:Primitives.SchemaElement list) schema =             
            let tid = System.Threading.Thread.CurrentThread.ManagedThreadId
            let storage = {connection = None
                           sortedTableBuilt=false
                           keyTypes = keyType;initTid = tid}
            lock x (fun _ -> 
                                if x.storage <> None then failwith (sprintf "other thread initialized %A" x.storage)
                                x.storage <- Some storage            
                                x.makeTempTable ()
                                x.AddSchema schema)
                                
        member private x.createCommand () =  
            let cmd =x.connection.CreateCommand ()
            cmd.CommandTimeout <- 10000
            cmd
                                                                  
        static member Create () =                   
            let tid = System.Threading.Thread.CurrentThread.ManagedThreadId
            let name = "Metrics_" + (System.Guid.NewGuid ()).ToString().Replace("{","").Replace("}","").Replace("-","") + string tid
            new MetricsStorageTable(name) |> up
            
        member private x.makeCountCommand () =            
                let cmd:System.Data.Common.DbCommand = x.createCommand ()            
                cmd.CommandText <- sprintf "SELECT COUNT(*) FROM %s" x.name 
                cmd
                    
        member public x.GetCount () : int64 = 
            use command = x.makeCountCommand ()
            let result = command.ExecuteScalar ()
            Database.dbIntToInt64 result            
        
        interface MetricsStorage.IMetricsStorage with
            member self.Close () = 
                match self.storage with 
                | Some info -> match info.connection with
                               | Some connection -> connection.Close ();connection.Dispose (); info.connection <- None
                               | None -> ()                               
                | None -> ()
                
                                        
            member x.AddSeq (keygen:(Primitives.Tuple -> Primitives.Datum list)) (tuples:Primitives.TupleSequence) =            
                                
                if (x.isInitialized = false) then                 
                    let schema = tuples.Schema
                    let tuple = schema.DefaultTuple
                    let keys = keygen tuple 
                    let keyTypes = keys |> List.map (fun key -> key.Typeof)
                    x.Init keyTypes tuple.schema
                              
                x.storage.Value.sortedTableBuilt <- false                
                use command = x.createCommand ()
                let maxInsert = (Db()).maxInsert
                let tups = new System.Collections.Generic.List<Primitives.Tuple> (maxInsert)
                x.makeInsertCommand command maxInsert
                for tup in tuples.tseq do   
                    tups.Add tup
                    if tups.Count = maxInsert then
                        x.assignMultiInsertCommand command keygen tups
                        command.ExecuteNonQuery () |> ignore
                        tups.Clear ()
                if tups.Count <> 0 then
                        use command = x.createCommand ()
                        x.makeInsertCommand command tups.Count
                        x.assignMultiInsertCommand command keygen tups
                        command.ExecuteNonQuery () |> ignore
                        tups.Clear ()
                                                                
            member x.Find (key:Primitives.Datum list) : Primitives.TupleSequence=
                if (x.isInitialized = false) then raise <| StorageFailure "Table not initialized"
                let sch = (up x).ReadSchema ()
                               
                let tseq =
                    seq {                                 
                        use command = x.makeFindCommand key
                        use r = command.ExecuteReader()
                        while r.Read() do                                                 
                            yield (r.[0] |> Serialization.decompressTuple sch)
                        r.Close ()
                        command.Dispose ()                
                    }
                Primitives.TupleSequence.Create sch tseq 
                
            member x.ReadSchema() : Primitives.Schema =
                if (x.isInitialized = false) then raise <| StorageFailure "Table not initialized"
                                                          
                use command = x.makeFindSchemaCommand ()
                use r = command.ExecuteReader()
                r.Read() |> ignore
                let bytes:byte[] = r.[0] |> unbox
                let memstr = new System.IO.MemoryStream (bytes)
                let formatter = Serialization.BinaryFormat
                let sch = memstr |> Serialization.deserializeSchema formatter
                r.Close ()
                command.Dispose ()                
                sch
                       
            member x.HasData () = x.isInitialized
            member x.ReadKeysAndValues () : (Primitives.Datum list * Primitives.Tuple) seq =            
                if (x.isInitialized = false) then raise <| StorageFailure "Table not initialized"                
                let nKeys = x.getKeyFields.Length
                let sch = (x |> up).ReadSchema ()
                let count = x.GetCount ()
                
                let buildSortTable () = 
                    if not x.storage.Value.sortedTableBuilt then
                        use sortCmd = x.buildSortedTableCommand ()
                        sortCmd.ExecuteNonQuery () |> ignore
                        x.storage.Value.sortedTableBuilt <- true                        
                    else ()
                
                // sqlite has to do this outside of the sequence or else it gets angry
                // cause the db is locked (this can be called from multiple threads at once for
                // different tables. Sqlite only lets you write to one table at a time (it takes
                // a db-wide write lock).
                //if (Db()).dbType = Database.Sqlite then buildSortTable ()
                buildSortTable ()
                
                let waitHandle = new System.Threading.ManualResetEvent(false)
                let result = ref None
                let exn = ref None
                let readChunk (command:System.Data.Common.DbCommand) =
                    async {
                        result := None
                        try
                            try
                                use r = command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess)

                                let chunk =
                                    seq [while r.Read() do
                                            let keys = x.getKeyData r
                                            yield (keys, r.[nKeys] |> Serialization.decompressTuple sch)]
                                r.Close ()
                                result := Some chunk
                            with ex -> exn := Some ex
                        finally
                            waitHandle.Set () |> ignore
                    }

                                                                                                            
                seq {                                       
                    //if (Db()).dbType <> Database.Sqlite then buildSortTable ()
                                        
                    let readCount = ref 0L 
                    let chunkSize = 0x1000L
                    use command = x.makeReadCommand ()
                    command.CommandTimeout <- 1000 * 1000 *10
                    assignParam command "limit" chunkSize
                    assignParam command "start" 0
                    Async.Start (readChunk command)
                    while !readCount < count do
                        waitHandle.WaitOne () |> ignore
                        waitHandle.Reset () |> ignore

                        match !exn with
                        | None -> ()
                        | Some ex-> raise ex
                        let chunk = (!result).Value
                        
                        readCount := !readCount + chunkSize
                        if (!readCount) < count then
                            assignParam command "start" !readCount
                            Async.Start (readChunk command)
                        yield! chunk
                    }
                                        
            member x.Read() : Primitives.TupleSequence =
                
                if (x.isInitialized = false) then raise <| StorageFailure "Table not initialized"
                let schema = (x |> up).ReadSchema ()
                let kvseq = (x |> up).ReadKeysAndValues ()
                let tseq = kvseq |> Seq.map snd
                
                Primitives.TupleSequence.Create schema tseq                 
                                                    
            member x.JoinInner 
                (that:MetricsStorage.IMetricsStorage) 
                (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple) : Primitives.Tuple seq =
                let other = that :?> MetricsStorageTable
                if (x.isInitialized = false) then raise <| StorageFailure "Table not initialized"
                if (other.isInitialized = false) then raise <| StorageFailure "Other Table not initialized"                
                                        
                let lsch = (x |> up).ReadSchema ()
                let rsch = (other |> up).ReadSchema ()
                let tseq = 
                    seq {                                 
                        use command = x.makeInnerJoinCommand other                        
                        use r = command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess)                        
                        while r.Read() do 
                            let left = r.[0] |> Serialization.decompressTuple lsch
                            let right = r.[1] |> Serialization.decompressTuple rsch
                            let tup = combineTuples left right
                            yield tup
                        r.Close ()
                        command.Dispose ()                
                    }
                tseq 
                
            member x.JoinLeftOuter 
                (that:MetricsStorage.IMetricsStorage) 
                (combineTuples:Primitives.Tuple->Primitives.Tuple->Primitives.Tuple) : Primitives.Tuple seq =
                
                let other = that :?> MetricsStorageTable
                if (x.isInitialized = false) then raise <| StorageFailure "Table not initialized"
                if (other.isInitialized = false) then raise <| StorageFailure "Other Table not initialized"
                
                let lsch = (x |> up).ReadSchema ()
                let rsch = (other |> up).ReadSchema ()
                                                                                       
                let tseq = 
                    seq {
                        let s2 = (up other).ReadSchema ()            
                        let defaultR = s2.DefaultTuple           
                        use command = x.makeLeftOuterJoinCommand other                        
                        use r = command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess)
                        while r.Read() do 
                            let left = r.[0] |> Serialization.decompressTuple lsch
                            let right = 
                                match r.[1] with
                                | :? System.DBNull -> defaultR
                                | _ -> r.[1] |> Serialization.decompressTuple rsch
                            
                            let tup = combineTuples left right
                            yield tup
                        r.Close ()
                        command.Dispose ()                
                    }
                tseq 
