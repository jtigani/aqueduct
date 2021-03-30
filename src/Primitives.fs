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

/// The Primitives module contains the basic data building blocks of pipe operations relating to Tuples and Schemas
module public Primitives =
    
    /// Exception thrown when schema mismatch occurs ... for instance if you try to 
    /// use an int datum when a string is expected by the schema.
    exception SchemaValidation of string
    /// An ordered sequence is not ordered as it claims to be.
    exception OrderingValidation of string
            
    let inline internal List_contains lst v = List.tryFind (fun x -> v = x) lst <> None
    
    
    // Store hash code for schemas we've already validated before. It is ugly to put it out here
    // but Idon't know of another way to make it really a single instance.
    // note we use an immutable set here so that we don't have to worry about 
    // multiple threads
    let private validatedSchemasRef : Set<int> ref = ref Set.empty
                                            
    /// SchemaElement type. This type defines the primitive types that we can have in our tuples. A schema element is
    ///     either an int (we only have one size, 64-bit integers) a float (which is a double-precision floating point
    /// value, a string (a sequence of characters) or a tuple (which is a set of Datum elements conforming to a 
    /// schema -- more on tuples below.                  
    type SchemaElement = 
        /// Element type representing a 64-bit integer
        | IntElement 
        /// Element type representing a 64-bit unsigned integer
        | UnsignedElement       
        /// Element type representing double-precision floating point
        | FloatElement 
        /// Element type representing a string of characters.
        | StringElement 
        /// Element type representing a sequence of tuples with a particular schema.
        | SequenceElement of Schema
    with                    
    
        /// Given a Schema Element (i.e. a type) and a string, return the typed data represented.
        member x.CreateDatumFromString (str:string) =
            match x with 
            | IntElement -> Datum.Int(int64 str)  
            | UnsignedElement -> Datum.Unsigned(uint64 str)          
            | FloatElement -> Datum.Float(double str)
            | StringElement -> Datum.String(str)
            | SequenceElement _ -> failwith "can't create a tuple datum from string"
            
        /// Given a string and an unknown type, return the typed data best represented
        static member BestFit(value:string) =            
            let intResult:Ref<int64>  = ref 0L
            let floatResult:Ref<double>  = ref 0.0
            if (System.Int64.TryParse (value, intResult)) then  IntElement
            else if (System.Double.TryParse (value, floatResult)) then FloatElement
            else StringElement 
            
        /// Verify that a datum is of a particular type. Raise a schema validation error if it is not
        member x.Check (datum:Datum) =             
            match x with
            | IntElement
            | UnsignedElement
            | FloatElement 
            | StringElement -> 
                if (datum.Typeof <> x) then 
                    raise <| SchemaValidation (sprintf "Schema element check failed %A <> %A" x datum)
            | SequenceElement sch -> 
                let innerSchema = (datum.asTuples).Schema 
                if innerSchema <> sch then
                    raise <| SchemaValidation (sprintf "Inner Schema element check failed %A <> %A" sch innerSchema)
        
         /// Creates an empty datum -- zero for ints, empty for strings and sequences, etc.
        member x.DefaultDatum =
            match x with 
            | IntElement -> Datum.IntZero
            | UnsignedElement -> Datum.UnsignedZero
            | FloatElement -> Datum.FloatZero
            | StringElement -> Datum.StringEmpty
            | SequenceElement sch -> TupleSequence.CreateUnsafe sch Seq.empty |> Datum.Tuples
        override x.ToString () = 
            match x with
            | IntElement -> "i"
            | UnsignedElement -> "u"
            | FloatElement -> "f"
            | StringElement -> "s"
            | SequenceElement sch -> sprintf "t:{%s}" (sch.ToString())
        
    /// Key describing the properties of subsequent tuples in a sequence. 
    /// Named fields can be ordered, and you can set whether 
    /// the entire key should be considered unique.
    /// The keys are ordered -- the first in the list is the 'most significant'.
    /// It is equivalent to a sql statement like 'order by (a,b,c)', where 
    /// a is the leading edge. If this key were unique, it would mean
    /// that no other tuple is going to have the same a,b,c combination.
    and SchemaKey =
        public {orderings:Ordering list;isUnique:bool}
    with 
        /// Create a non-unique key from orderings 
        static member Create(orderings) = {orderings=orderings;isUnique=false}
        /// Create a key from orderings and a flag saying whether it is a unique key
        static member Create(orderings,unique) = {orderings=orderings;isUnique=unique}
        /// Create a key where all fields are ascending (shortcut)
        static member CreateAscending((fields:string list),unique) = 
            {orderings = fields |> List.map (fun f -> Ascending,f);isUnique=unique}
        /// Empty key ... tuples aren't ordered.
        static member Empty = {orderings=[];isUnique=false}
        
        /// Returns a new key that is an intersection of two keys starting from
        /// the most significant field.
        /// For exmaple, if key 1 is ordered by [a,b] and key 2 is ordered by
        /// [a,b,c], the result would be ordered by [a]. 
        /// Also if either key is non-unique, the result will be non-unique.
        static member LeastRestrictive (leftKey:SchemaKey) (rightKey:SchemaKey) =
            let unique = leftKey.isUnique && rightKey.isUnique
            let matches = 
                let rec getMatches lord rord =
                    match lord,rord with
                    | lhd::ltl, rhd::rtl when lhd = rhd  -> lhd :: getMatches ltl rtl
                    | _,_ -> []
                getMatches leftKey.orderings rightKey.orderings
            SchemaKey.Create (matches,unique)                    
        /// Function to check for compatibility of a single ordering
        static member IsOrderingCompatible (x:Ordering) (order:Ordering) =
            let (xdir, xf) = x
            let (dir,f) = order
            xf = f && match (xdir,dir) with
                            | (a,b) when a=b -> true
                            | (Grouped,_) -> true
                            | _ -> false
                    
        /// Function to check whether the second key is compatible (i.e.
        /// more more restrictive than)
        static member AreOrderingsCompatible (xk:SchemaKey) (ok:SchemaKey) =
            let uniqCompat xu ou =
                match xu,ou with
                | false,_->true
                | true,true->true
                | true,false->false
                
            let rec ordCompat xord oord =
                match (xord,oord) with
                | ([],_) -> true
                | (_,[]) -> false
                | (a::xtail, b::otail) -> SchemaKey.IsOrderingCompatible a b && ordCompat xtail otail
            (ordCompat xk.orderings ok.orderings) && (uniqCompat xk.isUnique ok.isUnique)
            
        /// Checks to see if a schema is compatible with this key (i.e. is it sufficiently ordered 
        member x.IsOrdered (schema:Schema) =
            SchemaKey.AreOrderingsCompatible x schema.Key
        /// Create a new key with the unique flag set to true.
        member x.SetUnique u = {orderings=x.orderings;isUnique=u}
        /// Set the orderings (creates a new SchemaKey)
        member x.SetOrderings ords = {orderings=ords;isUnique=x.isUnique}        
        /// Get the field names
        member x.Fields = x.orderings |> List.map snd  
        /// Create a new key with a subset of fields. Preserves unique flag only if the subset
        /// is the full key
        member x.Subset flds =
            let ord = x.orderings |> List.filter (fun (_,f) -> List_contains flds f)
            SchemaKey.Create(ord,x.isUnique && (List.length x.orderings) = (List.length ord))
        /// Removes keys if they're there.
        member x.TryDrop flds =
            let ord = x.orderings |> List.filter (fun (_,f) -> not (List_contains flds f))
            SchemaKey.Create(ord,x.isUnique)        
        /// Renames fields.
        member x.Rename renames =
            let map = Map.ofList renames
            let renameFun (dir,fname) =  
                match Map.tryFind fname map with | Some newName -> (dir,newName) | None -> (dir,fname)
            let newOrd = x.orderings |> List.map renameFun
            SchemaKey.Create (newOrd,x.isUnique)        
        /// A singleton key is a unique key with no fields indicated. 
        member x.IsSingleton () = x.isUnique && (List.isEmpty x.orderings)
        override x.ToString () =
            sprintf "%A%s" x.orderings (if x.isUnique then "(u)" else "")
            
    /// Schema type -- a type framework for a tuple. Contains a set list of schema elements and the 
    /// names of those elements
    and 
        [<Sealed>]
        Column = 
        val private _name:string
        val private _element:SchemaElement
        val private _def:Datum
        private new(n,el,def) = {_name=n;_element=el;_def=def}
        
    with         
        static member Create(n,el,def) = new Column(n,el,def)
        static member Make(n,el) = new Column(n,el,el.DefaultDatum)
        member x.name = x._name
        member x.element = x._element
        member x.def = x._def
        member x.SetOrdering dir = new Column(x.name,x.element,x.def)
        member x.SetDefault def = new Column(x.name,x.element,def)
        member x.SetName name = new Column(name,x.element,x.def)
        member x.SetElement el = new Column(x.name,el,x.def)
        interface System.IComparable with
                member x.CompareTo other =                    
                    let that = other :?> Column
                    let namediff =  (x.name :> System.IComparable).CompareTo(that.name)
                    if namediff <> 0 then namediff else
                    let eldiff = (x.element :> System.IComparable).CompareTo(that.element)
                    if eldiff <> 0 then eldiff else
                    
                    match x.element,that.element with
                    | SequenceElement lts ,SequenceElement rts -> 0                        
                    | _,_ -> (x.def:> System.IComparable).CompareTo(x.def)
                                                                                                
        override x.Equals other = 
            let that = other :?> Column
            if (x.name = that.name) && (x.element = that.element) then
                match x.element ,that.element with
                | SequenceElement _ ,SequenceElement _ -> true
                | _,_-> x.def = that.def
            else false

        override x.GetHashCode () = x.name.GetHashCode() + x.element.GetHashCode()

    /// Schema is a collection of Columns describing the names and types of the data in a tuple. A schema 
    /// also has a SchemaKey which describes the ordering of the tuples in a sequence.
    and 
        [<Sealed>]
        Schema(comp:Column list,key:SchemaKey,map:System.Collections.Generic.Dictionary<string,Column*int>) = 
        static let schemaCache = Cache.RefCache  (fun sch -> sch |> Schema.ValidateColumns; sch)
        static let empty = new Schema([],SchemaKey.Empty,new System.Collections.Generic.Dictionary<string,Column*int>())
    with        
        interface System.IComparable with
                member x.CompareTo other =                    
                    let that = other :?> Schema
                    let compDiff = (x.Columns :> System.IComparable).CompareTo(x.Columns)
                    if compDiff <> 0 then compDiff else                    
                    let keydiff = (x.Key :> System.IComparable).CompareTo(that.Key)
                    keydiff

        override x.Equals other = 
            let that = (other :?> Schema)
            x.Columns.Equals(that.Columns) && x.Key.Equals(that.Key)
        override x.GetHashCode () =
            let rec loop (cmps:Column list) acc =
                match cmps with 
                | [] -> acc
                | cmp :: tl -> loop tl (cmp.GetHashCode () + acc)
            loop x.Columns 0 + x.Key.GetHashCode () 
                
        /// How many elements are in this schema
        member x.Arity = x.nameMap.Count
        
        /// Get the orderings from the key.
        member x.Ordering : Ordering list = x.Key.orderings
        
        /// Does this have some kind of ordering?
        member x.IsOrdered : bool = not x.Ordering.IsEmpty
        
        /// Get the schema key
        member x.Key:SchemaKey = key                         
        
        /// Gets the Schema Columns
        member x.Columns:Column list = comp
        member private x.nameMap : System.Collections.Generic.Dictionary<string,Column*int> = map
        member x.FieldOrdering fld = 
            if List_contains x.Key.Fields fld then
                x.Key.orderings  |> List.find (fun (_,n) -> n = fld)  |> fst
            else
                Unordered
        
        member inline x.Names = x.Columns |> List.map (fun comp -> comp.name) 
        member inline x.Elements =x.Columns |> List.map (fun comp -> comp.element) 
        member inline x.DefaultValues = x.Columns |> List.map (fun comp -> comp.def) 
        
        // note: if ordering by multiple fields, their order must be the same order
        // as the tuple.
        member x.SetOrdering (ord:Ordering list) = Schema.Create (x.Columns,SchemaKey.Create ord)        
        
        /// Create a new scheam with a different key.
        member x.SetKey(key:SchemaKey) = Schema.Create (x.Columns,key)
                                                  
        /// The empty schema.
        static member Empty = empty
          
        static member private ValidateColumns (schema:Schema) =
                let hashCode = schema.GetHashCode() 
                if Set.contains  hashCode (!validatedSchemasRef ) then () else
                let checkDtm (comp:Column) = 
                    match comp.element with 
                    | el when comp.def = el.DefaultDatum -> ()
                    | SequenceElement _ -> 
                        let tseq:TupleSequence = Datum.ExtractTuples comp.def
                        if not (Seq.isEmpty tseq.tseq) then failwith "Default value for tuple sequence in schema should be empty and is not."                    
                        else comp.element.Check comp.def
                    | _ -> comp.element.Check comp.def
                List.iter checkDtm schema.Columns 
                
                let names = schema.Columns |> List.map (fun comp -> comp.name) 
                let nameMap = names |> Set.ofList
                let (_,redundant) = 
                    List.foldBack (fun n (acc,res) -> if (Set.contains n acc) then (acc,n :: res) else (Set.add n acc, res) ) 
                                    names (Set.empty,[])
                if List.isEmpty redundant |> not then 
                    raise <| SchemaValidation (sprintf "Reused name %A (list of names: %A)" redundant names)
                
                schema.Key.orderings |> 
                    List.iter (fun (_,n)-> if not (Set.contains n nameMap) then 
                                                raise <| OrderingValidation (sprintf "Ordering mismatch: key %s not in %A" n names))
                validatedSchemasRef := Set.add hashCode (!validatedSchemasRef)
                ()
            
        /// Create a schema from a list of elements, names and default values 
        static member Create (components: Column list) =
            Schema.Create (components,SchemaKey.Empty)             
         
        /// Create a schema from columns and a key     
        static member Create (components:Column list, key:SchemaKey) =            
            let newSch = new Schema(components,key, Schema.CreateNameMap(components))
            schemaCache newSch
        
        static member private CreateNameMap (comps) =
            let dict = new System.Collections.Generic.Dictionary<string,Column*int>()
            comps |> Seq.iteri (fun idx (comp:Column) -> dict.[comp.name] <- (comp,idx))
            dict
        
        /// Create a schema from a list of name,element pairs                                    
        static member Make (nameAndElementList:(string * SchemaElement) list) =
            nameAndElementList |> List.map (fun (n,el)-> Column.Make(n,el)) |> Schema.Create                 
                    
        /// Creates a schema with different default values for named columns
        member x.AddDefaultValues (mappings: (string * Datum) list) =            
            let newComp = 
                List.map (fun (comp:Column) ->
                              let mapped = List.tryFind (fun (name, _) -> comp.name = name) mappings
                              match mapped with
                                        | Some (_, v) -> comp.SetDefault v
                                        | None -> comp
                        ) x.Columns 
            Schema.Create (newComp, x.Key)
                          
        /// Returns a schema for a subset of named columns.
        member x.ColumnsSchema (names: string list) =
            let newComp = List.map x.GetColumn names 
            // don't need to validate since we're just subsetting.
            Schema.Create(newComp,x.Key.Subset names)

        member x.ColumnsSchemaSimple (names: string list) = 
            List.map x.GetColumn names |> List.map (fun (sc:Column) -> sc.name,sc.element)
        
        member x.SchemaSimple =
            x.Columns |> List.map (fun (sc:Column) -> sc.name,sc.element)
            
        /// Returns the inner schema of a column which is a tuple sequence.
        member x.InnerSchema name = 
            let col = x.GetColumn name
            match col.element with
            | SequenceElement sch -> sch
            | _ -> raise <| SchemaValidation (sprintf "Wrong element type: %A. Tuple element required." (col.element))
           
        ///Add an element to the schema. Note we add it it to the beginning of the list.
        member x.Add (c: Column) = 
            Schema.Create(c :: x.Columns, x.Key)            
            
        /// Removes several columns from a schema
        member x.DropColumns names = 
            let exists n = List.tryFind (fun name -> n = name) names <> None
            let newComp = x.Columns |> List.filter (fun comp -> exists comp.name |> not) 
            // don't need to validate since we're just removing columns.
            let result = Schema.Create(newComp,x.Key.TryDrop names)
            if List.length result.Columns <> (List.length x.Columns) - (List.length names) then 
                raise <| SchemaValidation (sprintf "Could not remove %A from schema %A" names x)
            result
            
        /// Remove all columns except these named columns.
        member x.KeepColumns names = 
            let exists n = List.tryFind (fun name -> n = name) names <> None
            let newComp = x.Columns |> List.filter (fun comp -> exists comp.name) 
            let newSch = Schema.Create(newComp,x.Key.Subset names)
            if newSch.Arity <> List.length names then 
                raise (SchemaValidation (sprintf "missing names keeping %A in %A" names x.Names))
            newSch
        
        /// Tests whether the schema has a certain column
        member x.ContainsColumn name = x.nameMap.ContainsKey name
        
        /// Creates a new tuple by selecting columns that are in this schema
        member x.SelectColumns (tuple:Tuple) =
            x.Columns |> List.map (fun comp -> tuple.GetDatum comp.name) |> Tuple.Create x
           
        /// Get a tuple with default values conforming to this schema.
        member x.DefaultTuple = 
            Tuple.Create x x.DefaultValues
            
        /// Check to see whether a tuple conforms to a schema. An empty schema always passes the check.
        member x.Check (tup:Tuple) =
            if x.Arity = 0 then () else
            if x.Arity <> List.length tup.values then raise <| SchemaValidation (sprintf "Wrong number of elements (%d versus %d) in tuple %A for schema %A" (List.length tup.values) x.Arity tup x)
            List.iter2 (fun (comp:Column) dat -> comp.element.Check dat) x.Columns tup.values
            
        /// Update a schema with new entries. Columns are looked up by name and renamed if necessary.
        member x.Update (updates:(string * Column) list) =      
            let update name newcomp = 
                match (List.tryFind (fun (n,_) -> n=name) updates) with | None -> newcomp | Some (_,comp) -> comp
            let renames = updates |> List.map (fun(old,comp) -> old,comp.name)
            let components = List.map2 update x.Names x.Columns 
            let newKey = x.Key.Rename renames
            Schema.Create(components,newKey)
                                
        /// Concatenate two schema objects. The values from this one will come first.
        member x.Concat (schema:Schema) =
            let newComp = x.Columns @ schema.Columns 
            // keep the old key.
            Schema.Create(newComp,x.Key)
        
        /// Returns the index of a named column, or throw an exception if it cannot be found.
        member x.NameIndex name = x.nameMap.[name] |> snd
        
        /// Returns the index of a named column, None if it cannot be found
        member x.TryNameIndex name = 
            if x.ContainsColumn name then x.nameMap.[name] |> snd |> Some else None

        /// Returns the Column for a named column.
        member x.GetColumn name =             
            try 
                x.nameMap.[name] |> fst                 
            with :? System.Collections.Generic.KeyNotFoundException -> 
                raise <| SchemaValidation (sprintf "Schema element \"%s\" not found. Valid names are: %A" name x.Names)
              
        /// Gives us a meaningful string to see for debugging. 
        override x.ToString() = List.map (fun (comp:Column) -> sprintf "{%s:%A}" comp.name comp.element) 
                                                                          x.Columns |> sprintf "key=%s,comp=%A"  (x.Key.ToString())
                                                     
    /// A datum is a warpper around a primitive piece of data -- either an int, unsigned int, float, string, or tuple sequence. 
    /// Datums have a fixed type (see the typeOf method) but can be coerced to another type. A collection of datum objects plus
    /// a schema makes a tuple.
    and 
        [<Sealed>]
        Datum(datum:obj) =                     
            static let intCache = Cache.RefCache (fun (x:int64) -> Datum.CreateI x)
            static let floatCache = Cache.RefCache (fun (x:float) -> Datum.CreateF x)
            static let stringCache = Cache.RefCache (fun (x:string) -> Datum.CreateO x)
            static let intZero = Datum.Int 0L
            static let intOne = Datum.Int 1L
            static let unsignedZero = Datum.Unsigned 0UL
            static let unsignedOne = Datum.Unsigned 1UL
            static let floatZero = Datum.Float 0.0
            static let stringEmpty = Datum.String ""
                                                
        with
        /// get the inner int value of this datum. Throws exception if it isn't really an int
        /// If you want to convert to a different type, use SchemaElement.Coerce.
        member  x.asInt : int64 = let i64:int64 = datum |> unbox in i64     
        
        /// get the inner unsigned int value of this datum. Throws exception if it isn't really an unsigned int
        /// If you want to convert to a different type, use SchemaElement.Coerce.
        
        member  x.asUnsigned : uint64 = let ui64:uint64 = datum |> unbox in ui64 
          
        /// get the inner float value of this datum. Throws exception if it isn't really an float
        /// If you want to convert to a different type, use SchemaElement.Coerce.        
        member  x.asFloat : float = let f64:float= datum |> unbox in f64
        
        /// get the inner string value of this datum. Throws exception if it isn't really a string
        /// If you want to convert to a different type, use SchemaElement.Coerce.
        member  x.asString : string = datum :?> string
        
        /// Get the datum as a TupleSequence. It had better be a tuplesequence.
        member  x.asTuples : TupleSequence = datum :?> TupleSequence
        
        /// Static constants that are used often. These can be usefil
        /// for initializing things that are zero or one, or for doing 
        /// comparisons without boxing/unboxing.
        static member IntZero = intZero
        static member IntOne = intOne
        static member UnsignedZero = unsignedZero
        static member UnsignedOne = unsignedOne
        static member FloatZero = floatZero
        static member StringEmpty = stringEmpty
        
        member private x.getDatum:obj = datum
        
        interface System.IComparable with
            member x.CompareTo other =                    
                let that = other :?> Datum
                match datum with
                | :? string as s -> System.String.CompareOrdinal (s, (that.getDatum :?>  string))
                | _ -> let icomp = datum :?> System.IComparable
                       icomp.CompareTo(that.getDatum)                
                                                                                                
        override x.Equals other = 
            let that = other :?> Datum
            datum = that.getDatum

        override x.GetHashCode () = datum.GetHashCode()            
        
        /// Get the raw int value from a datum that we know is an Int 
        static member  ExtractInt (d:Datum) = d.asInt          
        /// Get the raw uint value from a datum that we know is an Unsigned
        static member ExtractUnsigned (d:Datum) = d.asUnsigned
        /// Get the raw float value from a datum that we know is a Float 
        static member  ExtractFloat (d:Datum) = d.asFloat                        
        /// Get the raw string value from a datum that we know is a string
        static member  ExtractString (d:Datum) = d.asString                        
        /// Get the raw tuple sequence from a datum that we know is a Tuples
        static member ExtractTuples (d:Datum) = d.asTuples
        
        static member private CreateI (i:int64) = new Datum(i |> box)
        static member private CreateU (u:uint64) = new Datum (u |> box)
        static member private CreateF (f:float) = new Datum(f |> box)
        static member private CreateO (o:obj) = new Datum(o)
        
        /// Create a String dataum from a string
        static member String (str:string) =
            // cache short strings
//            if str.Length <= 0x10 then 
//                stringCache str else
                Datum.CreateO str
        /// Create an Int datum from an int64
        static member Int (i:int64) : Datum = Datum.CreateI i //intCache i  
        /// Create an Unsigned datum from a uint64
        static member Unsigned (u:uint64) : Datum = Datum.CreateU u      
        
        /// Create a Float dataum from a double 
        static member Float (f:float) = Datum.CreateF f //floatCache f
        
        /// Create Tuples datum from a TupleSequence
        static member Tuples (ts:TupleSequence) = Datum.CreateO (ts)
                
        static member private Binop (d1:Datum) (d2:Datum) (intOp,uintOp,floatOp) (name:string) =
            match d1.Typeof ,d2.Typeof with
            | IntElement, IntElement -> Datum.Int(intOp (Datum.ExtractInt d1) (Datum.ExtractInt d2))
            | IntElement,_ -> raise <| SchemaValidation ("cannot " + name + " integer by " + string d2)
            | UnsignedElement, UnsignedElement -> Datum.Unsigned(uintOp (Datum.ExtractUnsigned d1) (Datum.ExtractUnsigned d2))
            | UnsignedElement,_ -> raise <| SchemaValidation ("cannot " + name + " unsigned integer by " + string d2)
            | FloatElement,FloatElement -> Datum.Float(floatOp (Datum.ExtractFloat d1) (Datum.ExtractFloat d2))            
            | FloatElement,_ -> raise <| SchemaValidation ("cannot " + name + "float by " + string d2)
            // todo: multiply like-shaped tuples
            | _,_  -> raise <| SchemaValidation ("cannot " + name + " " + string d1 + " by " + string d2)
            
        /// Multiply two datum objects and return a datum of the result
        static member Multiply d1 d2 = Datum.Binop d1 d2 ( (fun i j -> i * j),  (fun i j -> i * j), (fun i j -> i * j) ) "multiply"
        /// Add two datum objects and return a datum of the result
        static member Add d1 d2 = Datum.Binop d1 d2 ( (fun i j -> i + j), (fun i j -> i * j), (fun i j -> i + j) ) "add"
        /// Subtract two datum objects and return a datum of the result
        static member Subtract d1 d2 = Datum.Binop d1 d2 ( (fun i j -> i - j), (fun i j -> i * j), (fun i j -> i - j) ) "subtract"
            
        /// Report the SchemaElement type of a datum.
        member x.Typeof =
            match datum with
            | :? int64 -> IntElement
            | :? uint64 -> UnsignedElement
            | :? float -> FloatElement
            | :? string -> StringElement
            | :? TupleSequence as ts -> SequenceElement ts.Schema
            | _ -> failwith (sprintf "Unexpected datum type %A for: %A" datum.GetType datum)
            
        /// Given a datum and a type that we want it to be, coerce it to that other type.
        ///  If the type cannot be coerced , return a schema validation exception.
        member x.Coerce (el0,el1) =
            let isNA (s:string) = 
                match s.ToLowerInvariant () with
                | "na" -> true
                | @"n\a" -> true
                | @"\n" -> true                
                | _ -> false
                
            try 
                match (el0,el1) with 
                | (IntElement, IntElement) -> x
                | (IntElement, UnsignedElement) -> Datum.Unsigned(uint64 x.asInt)
                | (IntElement, FloatElement) -> Datum.Float(float x.asInt)
                | (IntElement, StringElement) -> Datum.String(string x.asInt)
                | (IntElement, SequenceElement(_)) -> raise <| SchemaValidation ("Cannot coerce signed int to tuple")
                | (UnsignedElement, IntElement) -> raise <| SchemaValidation ("Cannot coerce unsigned int to signed int")
                | (UnsignedElement, UnsignedElement) -> x
                | (UnsignedElement, FloatElement) -> Datum.Float(float x.asUnsigned)
                | (UnsignedElement, StringElement) -> Datum.String(string x.asUnsigned)
                | (UnsignedElement, SequenceElement(_)) -> raise <| SchemaValidation ("Cannot coerce unsigned int to tuple")
                | (FloatElement, IntElement) -> System.Math.Round x.asFloat |> int64 |> Datum.Int
                | (FloatElement, UnsignedElement) -> System.Math.Round x.asFloat |> uint64 |> Datum.Unsigned
                | (FloatElement, FloatElement) -> x
                | (FloatElement, StringElement) -> Datum.String(string x.asFloat)
                | (FloatElement, SequenceElement(_)) -> raise <| SchemaValidation ("Cannot coerce to tuple")
                | (StringElement, IntElement) -> Datum.Int(int64 x.asString)
                | (StringElement, UnsignedElement) -> Datum.Unsigned(uint64 x.asString)
                | (StringElement, FloatElement) -> if (isNA x.asString) then Datum.Float(System.Double.NaN) else Datum.Float(float x.asString)
                | (StringElement, StringElement) -> x
                | (StringElement, SequenceElement(_)) -> raise <| SchemaValidation ("Cannot coerce to tuple")
                | (SequenceElement _, _) -> raise <| SchemaValidation ("Cannot coerce from tuple")
            with 
            | :? System.FormatException  -> raise <| SchemaValidation (sprintf "Invalid format coercing %A to %A" x el1 )
            | :? System.OverflowException -> raise <| SchemaValidation ("Numeric overflow")
        override x.ToString () = (x.getDatum).ToString ()
     
    /// Ordering is a direction and a field name that is ordered 
    and Ordering= OrderDirection * string 
    /// Direction in which something should be (or is) ordered.
    and OrderDirection = Ascending | Descending | Grouped | Unordered    
                 
    /// Tuple type describes a generic typed tuple. A tuple is a list of Datum elements (which could be themselves tuples) 
    /// and a schema that describes the types and names of objects in the tuple.    
    and Tuple  = {values:Datum list;schema:Schema}
    with          
        member private x.nthValue n = x.values.[n]
        
        static member Float name (tup:Tuple) : float = tup.GetFloat name
        static member Int name (tup:Tuple) : int64 = tup.GetInt name
        static member Unsigned name (tup:Tuple) : uint64 = tup.GetUnsigned name
        static member String name (tup:Tuple) : string = tup.GetString name
        static member Tuples name (tup:Tuple) : TupleSequence = tup.GetTuples name
        static member Datum name (tup:Tuple) : Datum = tup.GetDatum name        
        
        /// How many elements are in the top level of the tuple
        member x.Arity = List.length x.values
        
        /// Get a Datum by column name
        member x.GetDatum (name:string) : Datum = 
            x.schema.NameIndex name |> List.nth x.values            
                    
        /// Try to get a datum with a name, but return None if it isn't found.
        member x.TryGetDatum name : Datum option =
            match x.schema.TryNameIndex name with
            | Some idx -> List.nth x.values idx |> Some
            | None -> None
                                
        /// Get an integer from a column name that we know refers to an Int
        member x.GetInt name = 
            try 
                x.GetDatum name |> Datum.ExtractInt
            with SchemaValidation msg -> raise <| SchemaValidation (sprintf "Error reading %s in %A. %s" name x.schema msg) 
            
        /// Get an unsigned integer from a column name that we know refers to an Unsigned 
        member x.GetUnsigned name = 
            try 
                x.GetDatum name |> Datum.ExtractUnsigned
            with SchemaValidation msg -> raise <| SchemaValidation (sprintf "Error reading %s in %A. %s" name x.schema msg) 
        
        /// Get a float from a column name that we know refers to a Float
        member x.GetFloat name =             
            try 
                x.GetDatum name |> Datum.ExtractFloat
            with SchemaValidation msg -> raise <| SchemaValidation (sprintf "Error reading %s in %A. %s" name x.schema msg) 
        
        /// Get a string from a column name that we know refers to a String
        member x.GetString name =             
            try
                x.GetDatum name |> Datum.ExtractString
            with SchemaValidation msg -> raise <| SchemaValidation (sprintf "Error reading %s in %A. %s" name x.schema msg) 
        
        /// Get the nth datum from the tuple
        member x.GetIndexed index = x.nthValue index 
        
        /// Get a tuple sequence from a column name that we know refers to a Tuples
        member x.GetTuples name =
            try
                x.GetDatum name |> Datum.ExtractTuples            
            with SchemaValidation msg -> raise <| SchemaValidation (sprintf "Error reading %s in %A. %s" name x.schema msg)
                                                
        /// take a schema that is a superset of this tuple and add default values to
        /// this tuple so that it matches the input schema
        member x.FillSchema (schema:Schema) : Tuple =
            let fillOne (comp:Column) =
                if x.schema.ContainsColumn comp.name then x.GetDatum comp.name else comp.def
            let values = schema.Columns |> List.map fillOne
            Tuple.Create schema values
        
        /// Given a schema and a list of values as strings, create a tuple. Note will choke
        /// if there are any Tuples in the schema
        static member CoerceToSchema (schema:Schema) (tup:Tuple) =                        
            let coerced = List.map3 (fun (s0:Column)(dtm:Datum)  (s1:Column) -> dtm.Coerce (s0.element,s1.element)) 
                                    tup.schema.Columns tup.values schema.Columns
            {values = coerced; schema=schema}
            
        /// Primary way to create a tuple -- from a schem and a list of Datum objects
        static member Create (schema:Schema) (data:Datum list) : Tuple=
            let tup = {schema=schema;values=data}
            //schema.Check tup
            tup
            
                                                
        /// Create a tuple with unknown schema from a list of strings. We assume that they
        /// are all strings and assign them to string elements
        static member CreateFromStrings (strings:string list) =
            let comps = strings |>  List.mapi (fun i _ -> Column.Make("$" + string i, StringElement))
            let data = strings |> List.map Datum.String
            let schema = Schema.Create (comps)
            {values=data; schema=schema}
        

        /// Given a list of modifications, returns a new tuple with the 
        /// updated values . Note that you cannot change the schema with
        /// this function.
        member x.UpdateValues (updates:(string * Datum) list) =
            let values = Array.ofList x.values
            updates |> List.iter (fun (n,d) -> 
                let idx = x.schema.NameIndex n                
                values.[idx] <- d)
            {values=values |> Array.toList;schema=x.schema}                        
            
        /// Replace a Datum with a different valued datum in a Tuple. Note
        /// this shouldn't change the schema
        member x.UpdateValue name datum =
            let values = Array.ofList x.values            
            let idx = x.schema.NameIndex name
            values.[idx] <- datum
            {values=values |> Array.toList;schema=x.schema}        
                            
        /// Given a list of (schema component, datum), applies the update to columns matching the name.
        /// The schema component and the datum value are both replaced.
        member x.UpdateValuesWithSchema (sch:Schema) (updates: (string * Datum) list)  =
            let data =
                sch.Names |> List.map (fun name -> match updates |> List.tryFind (fun (n,_) -> n = name) with 
                                                   | Some (n,d) -> d
                                                   | None -> x.GetDatum name)            
            Tuple.Create sch data
                                                           
        /// Takes a number of tuples that should match a schema when concatenated
        /// and concats them together. This prevents having to create a new schema,
        /// which is a reasonably expensive operation
        static member ConcatToSchema (resultSchema:Schema) (tuples:Tuple list ) =             
            // first make sure the schemas as cmpatible
            let comps = tuples |> List.map (fun tup -> tup.schema.Columns) |> List.concat
            let rec checkComp (scomp:Column) (tcomp:Column) = 
                match scomp.element,tcomp.element with
                | SequenceElement s1, SequenceElement s2 -> List.iter2 checkComp s1.Columns s2.Columns
                | _ ,_ -> if scomp <> tcomp then raise (SchemaValidation (sprintf "Schema mismatch %A <> %A" scomp tcomp)) 
            //List.iter2 checkComp resultSchema.Columns comps
            // now build a list of all of the values and apply them to the schema
            let values = tuples |> List.map (fun tup -> tup.values) |> List.concat
            Tuple.Create resultSchema values
        
        /// take a list of tuples, build a map of names to datum values, then 
        /// apply thos to a schema. Overlaps are ok, but the expected result
        // should be in the first tuple in the list.
        static member FillFromSchema (resultSchema:Schema) (tuples:Tuple list ) =
            let rec lookupNameInTups (comp:Column) (tups:Tuple list) =
                    match tups.Head.TryGetDatum comp.name with
                    | Some dtm -> dtm
                    | None -> lookupNameInTups comp tups.Tail
            let rec lookupNameInComps (comps:Column list) (tups:Tuple list) acc =
                match comps with 
                | [] -> List.rev acc
                | comp :: cTl -> 
                    let dtm = (lookupNameInTups comp tups) :: acc
                    lookupNameInComps cTl tups dtm
                                                        
            let values =  lookupNameInComps resultSchema.Columns tuples []
            Tuple.Create resultSchema values
            
            
        override x.ToString () = 
            List.zip x.values x.schema.Names  |> List.map (fun (v,n) -> sprintf "%s:%s" n (v.ToString())) |> sprintf "%A"            
    end 
    /// One column of a tuple table, which is a more compact representation of a tuple sequence in memory.
    /// in general, you only need this if you really know that you need it.
    and internal TupleColumn = 
        IntColumn of int64 array | UnsignedColumn of uint64 array | FloatColumn of float array | StringColumn of string array | 
            TupleSequenceColumn of TupleSequence array |EmptyColumn |
            DatumColumn of Datum array
    
        with 
        member inline x.Length = 
            match x with 
            | IntColumn arr -> arr.Length
            | UnsignedColumn arr -> arr.Length
            | FloatColumn arr -> arr.Length
            | StringColumn arr -> arr.Length
            | TupleSequenceColumn arr -> arr.Length
            | DatumColumn arr -> arr.Length
            | EmptyColumn -> 0
        member inline x.Row idx = 
            match x with 
            | IntColumn arr -> new Datum(arr.[idx])// |> Datum.Int
            | UnsignedColumn arr -> new Datum(arr.[idx])
            | FloatColumn arr -> new Datum(arr.[idx])// |> Datum.Float
            | StringColumn arr -> arr.[idx] |> Datum.String
            | TupleSequenceColumn arr -> arr.[idx] |> Datum.Tuples
            | DatumColumn arr -> arr.[idx] 
            | EmptyColumn -> failwith "bad index"
            
                    
        static member internal Create (idx:int) el (tups:System.Collections.Generic.List<Tuple>) =
            let len = tups.Count
            //Array.init len (fun ii -> tups.[ii].GetIndexed idx) |> DatumColumn
            if len = 0 then EmptyColumn            
            else                           
                match el with
                | IntElement -> Array.init len (fun ii -> (tups.[ii].GetIndexed idx).asInt) |> IntColumn
                | UnsignedElement -> Array.init len (fun ii -> (tups.[ii].GetIndexed idx).asUnsigned) |> UnsignedColumn
                | FloatElement -> Array.init len (fun ii -> (tups.[ii].GetIndexed idx).asFloat) |>  FloatColumn
                | StringElement -> Array.init len (fun ii -> tups.[ii].GetIndexed idx) |> DatumColumn
                | SequenceElement sch ->
                    Array.init len (fun ii -> (tups.[ii].GetIndexed idx).asTuples |> TupleTable.CacheTseq) |> TupleSequenceColumn
                                             
    /// Alternate columnar representation of a sequence of tuples. A tuple will be a view of multiple columns. This is 
    /// really only important when you need fine grained control over tuple storage (this uses less memory for long
    /// sequences that must be in memory)
    and TupleTable =
        val internal table:TupleColumn array        
        val schema:Schema        
        private new(tab,sch) = {table=tab;schema=sch}
        with
        member inline internal x.RowCount = if x.table.Length = 0 then  0 else x.table.[0].Length
        member inline internal x.ColumnCount = x.table.Length
        member internal x.GetSeq () = 
            let rowMax = x.RowCount - 1
            let colCount= x.ColumnCount 
            let schema = x.schema
            let table = x.table            
            let rec build row col acc = 
                if col = colCount then acc |> List.rev
                else build row (col + 1) ((table.[col].Row row) :: acc)
                
            seq {
                for row in 0..rowMax do                    
                    let data = build row 0 []
                    yield Tuple.Create schema data 
                }
        member internal x.GetTseq () = x |> TableBacked            
                                        
        static member internal CreateFromList (sch:Schema) (tups:System.Collections.Generic.List<Tuple>) : TupleTable =
            let colCount = sch.Arity
            let table = Array.create colCount EmptyColumn
            let rec buildArray colIdx (comps:Column list) =
                if colIdx = colCount then ()
                else 
                    let comp = List.head comps
                    let tl = List.tail comps
                    table.[colIdx] <- TupleColumn.Create colIdx comp.element tups 
                    buildArray (colIdx + 1) tl
            buildArray 0 sch.Columns
            new TupleTable(table,sch)
            
        static member Create (tseq:TupleSequence) : TupleTable =
            let sch = tseq.Schema
            let tups = new System.Collections.Generic.List<Tuple>()
            tseq |> Seq.iter (fun tup -> tups .Add tup)            
            TupleTable.CreateFromList sch tups
                    
        /// Convert a TupleSequnece into a TupleTable representation which will use less memory.
        /// Note don't do this with large tuple sequences without chopping them into pieces first
        static member CacheTseq (tupseq:TupleSequence) = 
            let tableMin = 0x10
            match tupseq with
            | TableBacked tt -> tupseq
            | SeqBacked (sch,tseq) ->                 
                let tseqCached = tseq |> Seq.cache
                if Seq.length tseqCached < tableMin then tupseq // don't cache short sequences
                else 
                    let sch = tupseq.Schema
                    let tups = new System.Collections.Generic.List<Tuple> ()
                    tseqCached |> Seq.iter (fun tup -> tups .Add tup) 
                    
                    let tt = TupleTable.CreateFromList sch tups
                    tt |> TableBacked
                
        /// In order to save memory, will create tuple tables out of inner tuple sequences only 
        static member CacheInnerTseqs (tupseq:TupleSequence) = 
            let sch = tupseq.Schema
            let isCompTuples (comp:Column) = match comp.element with SequenceElement _ -> true | _ -> false
            let cacheTuples comp (value:Datum) = 
                if isCompTuples comp then value.asTuples |> TupleTable.CacheTseq |> Datum.Tuples else value
                
            // if no inner tuple sequences, don't worry...
            if not (sch.Columns |> List.exists isCompTuples) then
                tupseq
            else 
                let newSeq = 
                    seq {
                        for tup in tupseq.tseq do
                            let values = List.map2 cacheTuples sch.Columns tup.values 
                            yield Tuple.Create sch values
                    }
                newSeq |> TupleSequence.CreateUnsafe sch               
            
    /// A TupleSequence is a sequence of tuples all with the same schema. 
    ///   A tuple may have nested tuple sequences if one of its schema columns is
    ///   a SequenceElement. 
    ///   Top level tuple sequences should only be consumed once, while inner tuple
    ///   sequences can be consumed multiple times.       
    and         
        [<CustomComparison>]
        [<CustomEquality>]        
        TupleSequence  = TableBacked of TupleTable | SeqBacked of Schema * Tuple seq
                
        with                                        
            member x.tseq = 
                match x with 
                | TableBacked tt -> tt.GetSeq ()
                | SeqBacked (_,seq) -> seq
            member x.schema =             
                match x with 
                | TableBacked tt -> tt.schema
                | SeqBacked (sch,_) ->  sch
            member x.is_ordered = not <| List.isEmpty x.schema.Ordering
            member x.Schema = x.schema
            member x.NewSeq sch (seq:Tuple seq) = 
                 let newTseq = TupleSequence.checkedSeq sch seq
                 SeqBacked (sch,newTseq)
                 
            static member private orderedSeq tseq (sch:Schema) key =
                let newsch = sch.SetKey key
                let newseq = 
                    seq {
                        for tup in tseq do
                            yield Tuple.Create newsch tup.values
                    }
                SeqBacked (newsch,newseq)
            member x.AddKey (key:SchemaKey) =                                
                TupleSequence.orderedSeq x.tseq x.schema key
                
            static member private  checkedSeq (sch:Schema) (tseq:Tuple seq) =                 
                seq {
                    use enumerator = tseq.GetEnumerator ()
                    if enumerator.MoveNext () = false then () else
                    let tup = enumerator.Current
                    tup.schema.Check tup
                    sch.Check tup
                    yield tup
                    while enumerator.MoveNext () do yield enumerator.Current 
                }
            
            (* Standard method to create a tuple sequence from a schema *)
            static member Create (schema:Schema) (tseq:Tuple seq) : TupleSequence = 
                let protectedSeq = TupleSequence.checkedSeq schema tseq
                SeqBacked(schema,protectedSeq)
                                
            static member internal CreateUnsafe (schema:Schema) (tseq:Tuple seq) : TupleSequence =                 
                SeqBacked(schema,tseq)
                
            static member internal CreateFromTable (tt:TupleTable) = TableBacked tt
                
            /// Given a tuple sequence, create a tuple sequence overriding the schema in
            /// each tuple with a new key describing orderings without modifying the 
            /// tuples in any other way  *)
            static member CreateKeyed (schema:Schema) (tseq:Tuple seq) : TupleSequence =
                TupleSequence.orderedSeq tseq schema schema.Key
                
            interface System.Collections.Generic.IEnumerable<Tuple> with
                member x.GetEnumerator() = x.tseq.GetEnumerator()
            interface System.Collections.IEnumerable with
                member x.GetEnumerator() = x.tseq.GetEnumerator() :> System.Collections.IEnumerator
            interface System.IComparable with
                member x.CompareTo other =                    
                    let that = other :?> TupleSequence
                    let schemaDiff = (x.schema :> System.IComparable).CompareTo(that.schema)
                    if schemaDiff <> 0 then schemaDiff else
                    use enumL = x.tseq.GetEnumerator ()
                    use enumR = that.tseq.GetEnumerator ()                    
                    let rec loop () = 
                        match enumL.MoveNext (),enumR.MoveNext()  with
                        | true,true -> 
                            let diff = (enumL.Current :> System.IComparable).CompareTo(enumR.Current)
                            if diff = 0 then loop () else diff                            
                        | true,false -> 1
                        | false,true -> -1
                        | false,false -> 0
                    loop ()
                                                        
            override x.Equals that = (x :> System.IComparable).CompareTo that = 0
            override x.GetHashCode () = Seq.length x.tseq + x.schema.GetHashCode ()
        
    /// Active patterns that allow you to do pattern matching on the type of a datum.      
    let (|IntDatum|_|) (dtm:Datum) = if dtm.Typeof = IntElement then Some dtm.asInt else None
    /// Active patterns that allow you to do pattern matching on the type of a datum.
    let (|UnsignedDatum|_|) (dtm:Datum) = if dtm.Typeof = UnsignedElement then Some dtm.asUnsigned else None
    /// Active patterns that allow you to do pattern matching on the type of a datum.
    let (|FloatDatum|_|) (dtm:Datum) = if dtm.Typeof = FloatElement then Some dtm.asFloat else None
    /// Active patterns that allow you to do pattern matching on the type of a datum.
    let (|StringDatum|_|) (dtm:Datum) = if dtm.Typeof = StringElement then Some dtm.asString else None
    /// Active patterns that allow you to do pattern matching on the type of a datum.
    let (|TuplesDatum|_|) (dtm:Datum) = match dtm.Typeof with  SequenceElement _ -> Some dtm.asTuples | _ -> None
    
    /// A PipeContinuation is used by pipes to call into the next pipe. 
    type PipeContinuation = TupleSequence -> unit
    