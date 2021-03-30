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

// the names here are unfortunately forshortened to improve the compactness of the serialized data.
namespace Aqueduct.Internal.XmlSerialization

    open Aqueduct.Primitives 
    open System.Xml.Serialization

    // DatumSer: Serialization type for Datum
    [<XmlInclude(typeof<TupleSequenceSer>)>]        
    type DatumSer =        
            [<XmlElement("val")>]
            val mutable datum:obj
            //[<XmlElement("tseq")>]
            //val mutable tseq:TupleSequenceSer option
            
            member x.ToDatum el =                
                match el,x.datum with
                | IntElement,:? int64 -> Datum.Int (x.datum |> unbox)
                | UnsignedElement,:? uint64 -> Datum.Unsigned (x.datum |> unbox)
                | FloatElement,:? double -> Datum.Float (x.datum |> unbox)
                | StringElement,_  -> Datum.String (x.datum :?> string)
                | SequenceElement _,_ -> Datum.Tuples <|  (x.datum :?> TupleSequenceSer ).ToTupleSequence ()
                | _ -> failwith (sprintf "unexpected value %A" x.datum)
            new (dtm) ={datum=dtm}
            new () = {datum=null}
            static member FromDatum dtm el =
                let d =
                    match el with
                    | IntElement -> Datum.ExtractInt dtm |> box
                    | UnsignedElement -> Datum.ExtractUnsigned dtm |> box
                    | FloatElement -> Datum.ExtractFloat dtm |> box
                    | StringElement -> Datum.ExtractString dtm :> obj
                    | SequenceElement _ -> let tseq = Datum.ExtractTuples dtm in ((TupleSequenceSer.FromTupleSequence tseq) :> obj)
                new DatumSer (d)
    
    // ElementSer: Serialization type for SchemaElements
    and ElementSer =     
            [<XmlElement("typ")>]
            val mutable typ:obj
            with 
            new (typ) = {typ=(typ :> obj)}
            new () = {typ=null}        
            member x.ToElement () =
                match x.typ with
                | :? string as str ->
                    match str with
                    | "i" -> IntElement
                    | "u" -> UnsignedElement
                    | "f" -> FloatElement
                    | "s" -> StringElement
                    | _ -> failwith (sprintf "unexpected value %A" str)
                | :? SchemaSer as sser -> sser.ToSchema () |> SequenceElement
                | _ -> failwith (sprintf "unexpected value %A" x.typ)
            static member FromElement el =
                let o = 
                    match el with
                    | IntElement -> "i" :> obj
                    | UnsignedElement -> "u" :> obj
                    | FloatElement -> "f" :> obj
                    | StringElement -> "s" :> obj
                    | SequenceElement sch -> SchemaSer.FromSchema sch :> obj
                new ElementSer (o)

    // OrderingSer: Serialization type for Orderings
    and OrderingSer = 
        [<XmlAttribute("n")>]
        val mutable n:string            
        [<XmlElement("o")>]
        val mutable o:char      
        new () = {n=System.String.Empty;o='u'}
        with new (n,o) = {n=n;o=o}        
             member x.ord () = 
                match x.o with
                | 'a' -> Ascending
                | 'd' -> Descending
                | 'g' -> Grouped
                | 'u' -> Unordered
                | _ -> failwith "unexpected ordering"
             static member OrderChar ord =
                match ord with
                | Ascending -> 'a'
                | Descending -> 'd'
                | Grouped -> 'g'
                | Unordered -> 'u'
             member x.toOrdering () = x.ord (),x.n
             static member fromOrdering (dir,n) =
                 new OrderingSer(n,OrderingSer.OrderChar dir)
    // OrderingSer: Serialization type for SchemaKeys
    and KeySer =         
            [<XmlElement("ords")>]
            val mutable os:OrderingSer array            
            [<XmlAttribute("uniq")>]
            val mutable uniq:bool      
            new () = {os=Array.empty;uniq=false} 
            with 
                static member FromKey (key:SchemaKey) =
                    let ks = new KeySer()
                    ks.uniq <- key.isUnique
                    ks.os <- key.orderings |> List.map OrderingSer.fromOrdering |> Array.ofList
                    ks
                member x.ToKey() =
                    let ords = 
                        if x.os = null then  [] else  x.os |> Array.map (fun os->  os.toOrdering ()) |> List.ofArray
                    SchemaKey.Create(ords,x.uniq)
                    
    // ColumnSer: Serialization type for.Columns
    and ColumnSer = 
            [<XmlAttribute("n")>]
            val mutable n:string
            [<XmlElement("el")>]
            val mutable  el:ElementSer
            [<XmlElement("d")>]
            val mutable d:DatumSer              
            new () = {n=System.String.Empty;el=new ElementSer();d=new DatumSer()}
            with new (n,e,d) = {n=n;el=e;d=d}        
                 
        
    // SchemaSer: Serialization type for Schema
    and  [<XmlRoot("sch")>] SchemaSer =
            [<XmlElement("cmp")>]
            val mutable components: ColumnSer array
            [<XmlElement("key")>]
            val mutable key: KeySer            
            with
            new (components,key) = {components=components;key=key}
            new () = {components=Array.empty;key=new KeySer()}
            member x.ToSchema ()  : Schema =                 
                let newcomp = x.components |> Array.map (fun comp -> let newElement =comp.el.ToElement () 
                                                                     Column.Create(comp.n, newElement,comp.d.ToDatum newElement)) |> List.ofArray
                let key = x.key.ToKey ()
                Schema.Create (newcomp,key)
            static member FromSchema sch : SchemaSer =
                let scomp = sch.Columns |> List.map (fun comp ->  new ColumnSer (comp.name,ElementSer.FromElement comp.element,DatumSer.FromDatum comp.def comp.element))
                let ks = KeySer.FromKey sch.Key
                new SchemaSer (scomp |> Array.ofList,ks)

    
    // TupleSer: Serialization type for Tuples (actually we only store the values not the schema).
    and [<XmlRoot("tup")>] TupleSer =
            [<XmlElement("data")>]
            val mutable data:DatumSer array 
            with 
            new(data) = {data=data}
            new() = {data=Array.empty}        
            member x.ToTuple (sch:Schema) = 
                let d =x.data |>  Seq.map2 (fun el (dtm:DatumSer)  -> dtm.ToDatum el) sch.Elements |> List.ofSeq
                Tuple.Create sch d
            static member FromTuple (tup:Tuple)=                
                let d =tup.values |> List.map2 (fun el v -> DatumSer.FromDatum v el) tup.schema.Elements |> Array.ofList
                new TupleSer(d)        
            
    // TupleSequenceSer: Serialization type for TupleSequence
    and [<XmlRoot("tseq")>] TupleSequenceSer =
            [<XmlElement("sch")>]
            val mutable sch:SchemaSer            
            [<XmlElement("dataSeq")>]        
            val mutable tupSeq:TupleSer array
            with 
            new (sch,data) = {sch=sch;tupSeq=data}
            new () = {sch=new SchemaSer();tupSeq=Array.empty}
            
            member x.ToTupleSequence () =
                let schema = x.sch.ToSchema ()                
                let tupseq = 
                    if x.tupSeq <> null then
                        seq [for tupleSer in x.tupSeq do yield tupleSer.ToTuple schema]
                    else Seq.empty
                TupleSequence.Create schema tupseq
            static member FromTupleSequence (tseq:TupleSequence) : TupleSequenceSer =
                let sch = SchemaSer.FromSchema tseq.Schema
                let data = [| for tup in tseq.tseq do yield TupleSer.FromTuple tup|]                
                new TupleSequenceSer(sch, data)
