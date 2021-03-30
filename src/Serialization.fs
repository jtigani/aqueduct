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

    open Primitives     
    open System.Xml.Serialization

/// This module contains routines for serializing tuples, sequences, and tupleseequences to streams    
module Serialization =        
    
    type internal Formatter<'a> = {ser:((System.IO.Stream*'a)->unit);deser:(System.IO.Stream->'a)}            
        
    let private XmlFormatter<'a> () = 
        let serializer = new System.Xml.Serialization.XmlSerializer( typeof<'a> );        
        let ser (str:System.IO.Stream, obj:'a) = 
            let writer = new System.Xml.XmlTextWriter(str, System.Text.Encoding.UTF8)
            writer.Formatting <- System.Xml.Formatting.Indented            
            serializer.Serialize (writer,(obj :> obj))
        let deser (str:System.IO.Stream) = serializer.Deserialize(str) :?> 'a
        {ser=ser;deser=deser}
     
       
    type SerializationFormat = XmlFormat | BinaryFormat
        with
        member internal x.GetFormatter<'a> () =
            match x with
            | XmlFormat -> XmlFormatter<'a> ()
            | BinaryFormat -> failwith "oops"

    let serializeSchema (format:SerializationFormat) (writer:System.IO.Stream) (schema:Schema) =
        match format with 
        | XmlFormat ->
            let formatter = format.GetFormatter<Internal.XmlSerialization.SchemaSer> ()
            let schobj = schema |> Internal.XmlSerialization.SchemaSer.FromSchema
            formatter.ser(writer, schobj)
        | BinaryFormat -> Internal.BinarySerialization.Serialize.serializeSchemaToStream writer schema
                
    // serializeTuple does not write the schema. YOu need a schema to deserialize                                
    let serializeTuple (format:SerializationFormat) (stream :System.IO.Stream)  (tuple:Tuple) =
        match format with 
        | XmlFormat ->
            let valuesObj = tuple |> Internal.XmlSerialization.TupleSer.FromTuple
            let formatter = format.GetFormatter<Internal.XmlSerialization.TupleSer> ()
            formatter.ser(stream, valuesObj)
        | BinaryFormat -> Internal.BinarySerialization.Serialize.serializeTupleToStream stream tuple
            
                            
    let serializeTupleSequence (format:SerializationFormat) (stream:System.IO.Stream)  (tseq:Primitives.TupleSequence) =        
        match format with
        | XmlFormat -> 
            let formatter = format.GetFormatter<Internal.XmlSerialization.TupleSequenceSer> ()
            formatter.ser(stream, Internal.XmlSerialization.TupleSequenceSer.FromTupleSequence tseq)
        | BinaryFormat -> Internal.BinarySerialization.Serialize.serializeTupleSequenceToStream stream tseq
               
    let rec public deserializeSchema (format:SerializationFormat) (stream:System.IO.Stream) =                
        match format with
        | XmlFormat ->
            let formatter = format.GetFormatter<Internal.XmlSerialization.SchemaSer> ()
            (formatter.deser stream).ToSchema ()
        | BinaryFormat -> Internal.BinarySerialization.Deserialize.deserializeSchemaFromStream stream
                        
    let public deserializeTuple (sch:Schema) (format:SerializationFormat)  (stream:System.IO.Stream) =       
        match format with 
        | XmlFormat ->
            let formatter = format.GetFormatter<Internal.XmlSerialization.TupleSer> ()
            let data = formatter.deser stream
            data.ToTuple sch
        | BinaryFormat -> Internal.BinarySerialization.Deserialize.deserializeTupleFromStream stream sch

    let rec public deserializeTupleOpt sch (format:SerializationFormat)  (stream:System.IO.Stream) =
        try deserializeTuple sch format stream  |> Some
        with :? System.IO.EndOfStreamException -> None
    
    let public deserializeTupleSequence (format : SerializationFormat) (stream : System.IO.Stream) =
        match format with
        | XmlFormat ->
            let formatter = format.GetFormatter<Internal.XmlSerialization.TupleSequenceSer> ()
            (formatter.deser stream).ToTupleSequence ()
        | BinaryFormat -> Internal.BinarySerialization.Deserialize.deserializeTupleSequenceFromStream stream

    let private compress f v =
        let formatter = BinaryFormat
        use inner = new System.IO.MemoryStream()
        use writer = new System.IO.Compression.GZipStream(inner, System.IO.Compression.CompressionMode.Compress) :> System.IO.Stream
        v |> f formatter writer
        writer.Flush ()
        writer.Close ()
        inner.ToArray ()
    
    let private decompress f (obj:obj) =
        let bytes:byte[] = obj |> unbox
        let inner = new System.IO.MemoryStream (bytes)
        let stream = new System.IO.Compression.GZipStream (inner, System.IO.Compression.CompressionMode.Decompress) :> System.IO.Stream
        stream |> f BinaryFormat
        
    let private toBytes f v = 
        let formatter = BinaryFormat
        use writer = new System.IO.MemoryStream()        
        v |> f formatter writer
        writer.Flush ()
        writer.Close ()
        writer .ToArray ()
        
    let private fromBytes  f (obj:obj) =
        let bytes:byte[] = obj |> unbox
        let stream = new System.IO.MemoryStream (bytes)        
        stream |> f BinaryFormat
        
    /// Serialize a tuple sequence, compress it, and return a byte array
    let compressTuples (tups:Primitives.TupleSequence) = compress serializeTupleSequence tups
    /// Turn a byte array representing a compressed tuple sequence int a tuple sequence object    
    let decompressTuples obj = decompress deserializeTupleSequence obj
    /// Compress a single tuple and return a byte array
    let compressTuple (tup:Primitives.Tuple) = toBytes serializeTuple tup
    /// Decompress a byte array represenging a single tuple
    let decompressTuple schema obj = fromBytes (fun fmt str ->let t = deserializeTuple schema fmt str in str.Close ();t) obj
