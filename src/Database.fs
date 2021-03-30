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

/// This module contains database abstractions. Can be used to specify which database
/// to use by pipes. A db instance is used to create a database table backed sink or tap
/// or to specify a database to use for sorts and joins.
module public Database =

    /// this controls which database provider to use.  Default will be MySql
    type DbType = Sqlite | MySql 
                    
    /// Abstracts a database instance.
    type DbData = 
        val private connectionString:string
        val private dbType:DbType
        val mutable private factoryOpt:System.Data.Common.DbProviderFactory option
        private new (dbType,connectionString) = {connectionString=connectionString;dbType=dbType;factoryOpt=None}
        private new (dbType,connectionString,factory) = {connectionString=connectionString;dbType=dbType;factoryOpt=Some factory}
              
        with 
        /// Create a DbData from a known type and connection string.
        static member Create (dbType,connectionString) = new DbData(dbType,connectionString)
        /// Create a DbData with a given System.Data.Common.DbProviderFactory.       
        static member CreateWithFactory (dbType,connectionString,factory) = new DbData(dbType,connectionString,factory)
        
        /// Returns the name of this database.
        member public x.GetDatabaseName () = 
            let regex = new System.Text.RegularExpressions.Regex("Database=(?<db>[^;]+)")
            let regMatch = regex.Match x.connectionString
            regMatch.Groups.["db"].Value      
                                          
        member private x.GetFactory () = 
            if x.factoryOpt = None then 
                let factory = 
                    match x.dbType with
                    | Sqlite -> System.Data.Common.DbProviderFactories.GetFactory "System.Data.SQLite"                
                    | MySql -> System.Data.Common.DbProviderFactories.GetFactory "MySql.Data.MySqlClient1"
                x.factoryOpt <- Some factory
            x.factoryOpt.Value
          
        /// Create a database connection  
        member x.Connect () = 
            let connection = (x.GetFactory ()).CreateConnection ()
            connection.ConnectionString <- x.connectionString
            connection.Open ()
            connection                       
            
        /// Create a database data adapter
        member x.CreateDataAdapter() = (x.GetFactory ()).CreateDataAdapter ()
            
        member internal x.hackDbCreate (createSql:string) = 
            match x.dbType with 
            | Sqlite (_) -> createSql.Replace("AUTO_INCREMENT", "AUTOINCREMENT")
            | MySql (_) -> createSql.Replace("AUTOINCREMENT", "AUTO_INCREMENT")
            
        member internal x.paramText name:string =
            match x.dbType with 
            | Sqlite (_) -> "?"
            | MySql (_) -> "?" + name
            
        member internal x.maxInsert =
            match x.dbType with 
            | Sqlite (_) -> 1
            | MySql (_) -> 0x100
            
        member internal x.paramTextCompressed name:string =
            match x.dbType with 
            | Sqlite (_) -> "?"
            | MySql (_) -> "COMPRESS(?" + name + ")"
            
        member internal x.decompressText name:string = 
            match x.dbType with 
            | Sqlite (_) -> name
            | MySql (_) -> "UNCOMPRESS(" + name + ")"
            
        member internal x.paramName name:string =
            match x.dbType with 
            | Sqlite (_) -> name
            | MySql (_) -> "?" + name 
        
        member internal x.partialKey name len dir =
            match x.dbType with
            | Sqlite _ -> sprintf "%s %s" name dir // mysql doesn't support partial keys. Use the whole darn thing.
            | MySql _ -> sprintf "%s (%d) %s" name len dir
                
        member internal x.createTableWithPrimaryKey name colstr (pkeystr:string) =
            if pkeystr.Length = 0 then sprintf "CREATE TABLE %s (%s)" name colstr else
            match x.dbType with
            | Sqlite _ -> sprintf "CREATE TABLE %s (%s) %s" name colstr pkeystr
            | MySql _ -> sprintf "CREATE TABLE %s (%s,%s)" name colstr pkeystr
            
               
    let internal dbIntToInt64 dbi =
        match dbi:obj with
        | :? int32 -> let vint = unbox dbi in vint |> int64
        | :? int64 ->
            let vlong:int64 = dbi |> unbox in vlong
        | _ -> failwith "bad int"
 
    let internal dbUintToUint64 dbu =
        match dbu:obj with
        | :? uint32 -> let vuint = unbox dbu in vuint |> uint64
        | :? uint64 ->
            let vulong:uint64 = dbu |> unbox in vulong
        | _ -> failwith "bad unsigned int"
                                                  
    let internal dbIntToInt dbi =
        match dbi:obj with
        | :? int32 -> unbox dbi
        | :? int64 -> 
            let vlong:int64 = dbi |> unbox
            let vint = vlong |> int32 
            vint
        | _ -> failwith "bad int"        
        
    let internal dbFloatToFloat dbf =
        match dbf:obj with
        | :? System.Double -> let vdouble:System.Double = unbox<System.Double> dbf in vdouble
        | :? System.Single -> let vfloat:System.Single = unbox<System.Single> dbf in vfloat |> float            
        | _ -> failwith "bad float"
