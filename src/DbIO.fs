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



module public DbIO =
    open Aqueduct.Primitives
    open Aqueduct

    exception StorageFailure of string 
    
    
    type TableMode = TableAppend | TableCreate | TableReadOnly
        
    type  StorageInfo = 
        { schema:Schema          
          connections:System.Collections.Generic.Dictionary<int,System.Data.Common.DbConnection>
          initTid:int
        }
        with 
        static member Create(schema:Schema) = 
            let conn = new System.Collections.Generic.Dictionary<int,System.Data.Common.DbConnection> ()            
            {schema = schema;connections = conn;initTid = System.Threading.Thread.CurrentThread.ManagedThreadId}       
    
      
    
    
    type TupleTable=
        val private name:string
        val private mode:TableMode
        val private db:Database.DbData
        val mutable private storage:StorageInfo option
         
        private new (name:string,mode:TableMode,db) = {name=name;storage=None;mode=mode;db=db}        
                            
        static member Create(db:Database.DbData,tableName,mode) =
            new TupleTable(tableName,mode,db)
        static member Create(db:Database.DbData,tableName,mode,schema) =
            let tt = new TupleTable(tableName,mode,db)
            tt.Init schema
            tt
            
                
        member private x.addParam (cmd : System.Data.Common.DbCommand) (name:string) (o : obj) =
          let param = cmd.CreateParameter()
          param.ParameterName <- x.db.paramName name      
          param.Value <- o
          cmd.Parameters.Add(param) |> ignore      
        
        member private x.addReusableParam (cmd : System.Data.Common.DbCommand) (name:string)  =
          let param = cmd.CreateParameter()
          param.ParameterName <- x.db.paramName name            
          cmd.Parameters.Add(param) |> ignore      
        
        member private x.assignParam (cmd : System.Data.Common.DbCommand) (name:string) (o:obj) =
            let param = cmd.Parameters.[x.db.paramName name]
            param.Value <- o
            
        member private x.schema = x.storage.Value.schema
        
        member private x.makeTupleTableString = 
            let builder = new System.Text.StringBuilder()
            let typestr typ=                 
                match typ with                
                | Primitives.IntElement -> "BIGINT(20) NOT NULL" 
                | Primitives.UnsignedElement -> "BIGINT(20) UNSIGNED NOT NULL"
                | Primitives.FloatElement -> "REAL NOT NULL" 
                | Primitives.StringElement-> "TEXT NOT NULL"
                | Primitives.SequenceElement _ -> "BLOB NOT NULL"
                
            let typestrs= x.schema.Elements |> List.map typestr 
            let datastrs = List.map2 (fun name typ -> name + " " + typ) x.schema.Names typestrs |> Array.ofList
            let ordToName ord = 
                match ord with 
                | Ascending -> "ASC"
                | Descending -> "DESC"
                | Grouped -> "ASC"
                | Unordered -> failwith "shouldn't be ordered as unordered"
            let indexStr (ord,fld) =
                if (x.schema.GetColumn fld).element = Primitives.StringElement then 
                    x.db.partialKey fld 64 (ordToName ord)
                else 
                    sprintf "%s %s" fld (ordToName ord)
            let indexStrs = x.schema.Key.orderings |> List.map indexStr |> Array.ofList 
            let datastr = System.String.Join(",", datastrs)
            let indexStr = System.String.Join(",", indexStrs)
            let primaryKeyStr = 
                if Array.length indexStrs <> 0 && x.schema.Key.isUnique then                                             
                    sprintf " PRIMARY KEY (%s)" indexStr
                else ""
            let createIndexStr  = 
                if Array.length indexStrs <> 0 && not x.schema.Key.isUnique then                                             
                    sprintf "CREATE INDEX %s_index on %s (%s)" x.name x.name indexStr
                else ""                
                                                                    
            let len = x.name.Length
            builder.Append(sprintf "DROP TABLE IF EXISTS %s" x.name).
                    Append(";").
                    Append(x.db.createTableWithPrimaryKey x.name datastr primaryKeyStr).
                    Append(";").
                    Append(createIndexStr) |> ignore
                    
            builder.ToString();
            
        member private x.connection = 
            let tid = System.Threading.Thread.CurrentThread.ManagedThreadId
            if not (x.storage.Value.connections.ContainsKey tid) then 
                let connection = x.db.Connect ()                
                x.storage.Value.connections.Add (tid,connection)
            x.storage.Value.connections.[tid]
                            
        member private x.makeTupleTable () =
            use command = x.connection.CreateCommand()            
            command.CommandText <- x.makeTupleTableString 
            command.ExecuteNonQuery () |> ignore
            ()
                
        member private x.makeValue el (dtm:Primitives.Datum) =
            match el with 
            | IntElement -> box (Datum.ExtractInt dtm)
            | UnsignedElement -> box (Datum.ExtractUnsigned dtm)
            | FloatElement -> box (Datum.ExtractFloat dtm)  
            | StringElement -> Datum.ExtractString dtm :> obj 
            | SequenceElement _ -> (Serialization.compressTuples (Datum.ExtractTuples dtm)) :> obj
            
        member private x.parseDataType (typ:System.Type) =
            if typ.IsAssignableFrom(typeof<int>) || typ.IsAssignableFrom(typeof<int16>) 
                || typ.IsAssignableFrom(typeof<int64>) then IntElement 
            else if typ.IsAssignableFrom(typeof<float>) || typ.IsAssignableFrom(typeof<System.Single>) then FloatElement
            else if typ.IsAssignableFrom(typeof<string>) || typ.IsAssignableFrom(typeof<byte[]>) then StringElement
            else raise (StorageFailure (sprintf "cannot infer type %A from schema %s" typ x.name))
            
        member private x.GetDatum (comp:Column) (obj:obj) =
            match obj with
            | :? System.DBNull -> comp.def
            | _ -> 
                match comp.element with                                
                | Primitives.IntElement -> obj|> Database.dbIntToInt64 |> Primitives.Datum.Int 
                | Primitives.UnsignedElement -> obj|> Database.dbUintToUint64 |> Primitives.Datum.Unsigned 
                | Primitives.FloatElement ->  obj |> Database.dbFloatToFloat |> Primitives.Datum.Float
                | Primitives.StringElement-> (obj :?> string) |> Primitives.Datum.String 
                | Primitives.SequenceElement _ -> (obj :?> byte[]) |> Serialization.decompressTuples |> Primitives.Datum.Tuples
        
        member private x.getTuple (reader: System.Data.Common.DbDataReader) =
            let getValue index (comp:Column)  =
                x.GetDatum comp (reader.GetValue index )
                
            x.schema.Columns |> List.mapi getValue |> Tuple.Create x.schema
        
        member private x.makeInsertCommand  (cmd:System.Data.Common.DbCommand) =
            
            let valueStr  = System.String.Join (",", [| for comp in x.schema.Columns do yield x.db.paramText comp.name |])
            cmd.CommandText <- sprintf "INSERT INTO %s VALUES (%s)" x.name valueStr
            for comp in x.schema.Columns do x.addReusableParam cmd comp.name            
            cmd.Prepare ()
            ()
                                    
        member private x.assignInsertCommand (cmd:System.Data.Common.DbCommand) (tuple:Primitives.Tuple) =
            let assignOne (comp:Column) dtm = 
                let dtmObj = x.makeValue comp.element dtm
                x.assignParam cmd comp.name dtmObj
            List.iter2 assignOne tuple.schema.Columns tuple.values       
                                
        member private x.makeReadCommand  (start,count) =            
            let cmd:System.Data.Common.DbCommand = x.createCommand ()
            let fieldstr = 
                System.String.Join (",", [| for comp in x.schema.Columns do yield comp.name |])
            let keystr = 
                if x.schema.IsOrdered then 
                    " ORDER BY " + System.String.Join (",", [| for field in x.schema.Key.Fields do yield field |])
                else 
                    ""
            cmd.CommandText <- sprintf "SELECT %s FROM %s%s LIMIT %d,%d" fieldstr x.name keystr start count
            cmd
            
        member private x.makeCountCommand () =            
            let cmd:System.Data.Common.DbCommand = x.createCommand ()            
            cmd.CommandText <- sprintf "SELECT COUNT(*) FROM %s" x.name 
            cmd
                    
        member private x.isInitialized = x.storage <> None
        
        member private x.Add (command:System.Data.Common.DbCommand) (tuple:Primitives.Tuple) =
            x.assignInsertCommand command tuple
            command.ExecuteNonQuery () |> ignore
            
        member private x.Init schema =             
            let tid = System.Threading.Thread.CurrentThread.ManagedThreadId
            let storage = StorageInfo.Create (schema)
                        
            if x.storage <> None then failwith (sprintf "other thread initialized %A" x.storage)
            x.storage <- Some storage            
            match x.mode with 
            | TableCreate -> x.makeTupleTable ()                    
            | _ -> ()            
                                            
        member private x.createCommand () =  x.connection.CreateCommand ()
        
        static member private InferSchema (tt:TupleTable) =
            use connection = tt.db.Connect ()                     
            use cmd:System.Data.Common.DbCommand = connection.CreateCommand ()
            cmd.CommandText <- sprintf "SELECT * FROM %s LIMIT 0" tt.name
            use reader = cmd.ExecuteReader ()
            use table = reader.GetSchemaTable ()
            let comps =
                [for ii in 0..reader.FieldCount-1 do                    
                    let name = reader.GetName ii
                    let el = reader.GetFieldType ii |> tt.parseDataType
                    let comp = Column.Create(name,el,el.DefaultDatum)
                    yield comp
                ]
                
            let key = 
                if table.PrimaryKey.Length <> 0 then 
                    let keys = [for keyCol in table.PrimaryKey do yield Ascending,keyCol.ColumnName]                    
                    SchemaKey.Create (keys,true) // primary keys are unique
                else
                    // unfortunately, if there is no primary key, we don't know anything else
                    // about the key.                    
                    SchemaKey.Empty
            
            Schema.Create(comps,key) |> tt.Init
                                                                                                                          
        member x.Close () = 
            match x.storage with 
            | Some info -> let connections = List.ofSeq info.connections.Values
                           for connection in connections do connection.Close (); 
                           info.connections.Clear ()
            | None -> ()
            
        member public x.Write (tuples:Primitives.TupleSequence) =            
            if (x.isInitialized = false) then                 
                let schema = tuples.Schema
                if x.mode = TableReadOnly then 
                    raise (StorageFailure (sprintf "Cannot write to %s. It was opened for read-only."  x.name))
                x.Init schema
                                                
            let tx = ref <| x.connection.BeginTransaction ()
            use command = (!tx).Connection.CreateCommand ()
            let tupsInTx  = ref 0
            try                 
                x.makeInsertCommand command
                // simulate a do-while loop... we've already pulled off the head of the seq,
                // so we're in for the whole thing...
                for tup in tuples.tseq do   
                    x.Add command tup
                    
                    // we can only write so much at once to the database or else
                    // we hit size limits. So break up transactions that get very large.
                    tupsInTx := !tupsInTx + 1
                    if (!tupsInTx > 1024) then
                        (!tx).Commit ()
                        (!tx).Dispose ()
                        tx := x.connection.BeginTransaction ()
                        tupsInTx := 0                        
                
                if !tupsInTx<> 0 then 
                    (!tx).Commit ()
                    tupsInTx:= 0
            finally 
                if !tupsInTx<> 0 then
                    (!tx).Rollback ()
                (!tx).Dispose()
         
        member public x.GetCount () : int64 = 
            use command = x.makeCountCommand ()
            let result = command.ExecuteScalar ()
            Database.dbIntToInt64 result            
            
                                           
        member public x.Read() : Primitives.TupleSequence =            
            if (x.isInitialized = false) then 
                // if nobody as told us what schema to use, try to infer it.
                if x.mode = TableCreate then 
                    raise (StorageFailure (sprintf "Cannot read from %s opened for create without writing first."  x.name))
                // we have to infer the schema since nobody has told us.
                TupleTable.InferSchema x
                                        
            let count = x.GetCount ()
            
            let tseq = seq {                                 
                let readCount = ref 0L 
                let chunkSize = 0x10000L
                while !readCount < count do
                    use command = x.makeReadCommand (!readCount,chunkSize)
                    command.CommandTimeout <- 1000 * 1000 *10                                        
                    use r = command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess)
                    yield! seq [while r.Read() do yield x.getTuple r]
                    r.Close ()     
                    readCount := !readCount + chunkSize
                }            
            Primitives.TupleSequence.Create x.schema tseq