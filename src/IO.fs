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
#light
namespace Aqueduct

/// This module implements reading and writing to files 
module public IO =    
    open Aqueduct.Primitives
    open Aqueduct.Pipes
         
    /// Interface for a file-based data format 
    type FormatInfo = 
        {readSeq:System.IO.Stream->TupleSequence;
         writeSeq:System.IO.Stream->TupleSequence->unit}
         
    /// FormatInfo reading and writing to specially formatted xml files 
    let XmlFormatInfo=         
        let formatter = Serialization.XmlFormat
        {readSeq=Serialization.deserializeTupleSequence formatter;writeSeq=Serialization.serializeTupleSequence formatter}

    /// FormatInfo reading and writing to binary-encoded files.
    let BinaryFormatInfo=
        let formatter = Serialization.BinaryFormat
        {readSeq=Serialization.deserializeTupleSequence formatter;writeSeq=Serialization.serializeTupleSequence formatter}
    
    /// Wraps another format info and create a format info that will compress/decompress.    
    let CompressedFormatInfo (inner:FormatInfo) = 
        {readSeq = fun stream -> 
                        let defstr = new System.IO.Compression.GZipStream (stream, System.IO.Compression.CompressionMode.Decompress) :> System.IO.Stream
                        inner.readSeq defstr
            
         writeSeq = fun stream tseq ->
                        let defstr = new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Compress) :> System.IO.Stream
                        inner.writeSeq defstr tseq                        
                        defstr.Flush ()
                        defstr.Close ()
        }
        
    /// FormatInfo for reading and writing to csv files. IF the schema is known in advance, you
    /// can pass Some schema, otherwise use None .
    let CsvFormatInfo (schOpt:Schema option) = 
        {readSeq=CsvIO.readTuplesFromCsv schOpt;writeSeq=CsvIO.writeTuplesAsCsv}
        
    /// FormatInfo for reading and writing to tab separated files. IF the schema is known in advance, you
    /// can pass Some schema, otherwise use None .
    let TabFormatInfo (schOpt:Schema option) = 
        {readSeq=CsvIO.readTuplesFromTab schOpt;writeSeq=CsvIO.writeTuplesAsTab}
        
    /// FormatInfo when you don't actually want to or write read anything (read produces an empty sequence 
    /// with an empty schema, write consumes all data and throws it away).
    let NullFormatInfo =
        {readSeq=(fun _ -> TupleSequence.Create Schema.Empty Seq.empty);writeSeq=(fun _ seq -> for _ in seq do ())}
                        
    /// Returns a tap from a csv file. The csv file should have a header row describing 
    /// the columns. All columns will be strings in the schema, you can apply a schema at a
    /// later stage if you know other colunmns should be more restrictive data types (such
    /// as ints)
    let private load (formatInfo:FormatInfo) streamFun =
        Tap.Create (fun () -> streamFun () |> formatInfo.readSeq) 
              
    /// Thrown when we don't recognize a file format                            
    exception FileFormatNotFound of string
    /// determine file format from a file name
    /// Supported formats:
    /// csv : comma-separated
    /// tab : tab-separated
    /// bin : aqueduct custom binary format
    /// xml : aqueduct custom xml format
    /// nul : empty file
    /// *.gz : combine with another format for compressed (gzipped) files
    let rec formatFromFileName (fname:string) =
        let formatFromExtension (ext:string) =                                        
            match ext with
            | ".bin" -> BinaryFormatInfo
            | ".xml" -> XmlFormatInfo
            | ".tab" -> TabFormatInfo None 
            | ".tsv" -> TabFormatInfo None
            | ".csv" -> CsvFormatInfo None
            | ".null" -> NullFormatInfo
            | ".nul" -> NullFormatInfo        
            | _ -> raise <| FileFormatNotFound ("Unrecognized format info: " + ext)
            
        let fname = fname.TrimEnd().ToLowerInvariant ()
        if fname.Equals ("-") then // stdin / out
            TabFormatInfo None
        else if fname.EndsWith (".gz")  then 
            CompressedFormatInfo (formatFromFileName (fname.Substring(0,fname.Length - 3)))        
        else 
            System.IO.Path.GetExtension(fname)|> formatFromExtension

    /// Given a format info and an already opened stream, return a Pipe ITap that
    /// is will generate tuples from the stream 
    let public readFromStream (formatInfo:FormatInfo) (stream:System.IO.Stream)  =
        load formatInfo (fun () -> stream)
        
    /// Given a format info and a named file, return a Pipe ITap that
    /// is will generate tuples from the stream 
    let public readFromFile (formatInfo:FormatInfo) (fileName:string) =
        let up = match fileName with
                 | "-" -> (fun () -> System.Console.OpenStandardInput())
                 | "" -> (fun () -> new System.IO.MemoryStream(Array.create 0 (byte 0)) :> System.IO.Stream)
                 | _ -> if not (System.IO.File.Exists (fileName)) then 
                                failwith (sprintf "Input file not found: %s" fileName)
                        (fun () -> new System.IO.FileStream(fileName,
                                                     System.IO.FileMode.Open,
                                                     System.IO.FileAccess.Read,
                                                     System.IO.FileShare.ReadWrite,
                                                     0x10000) :> System.IO.Stream)
        load formatInfo up
    
    /// Given a fully-qualified url, create a tap
    let public readFromWeb (formatInfo:FormatInfo) (url:string) =
        let streamGen () = 
            let uri:System.Uri = new System.Uri(url)
            let req = System.Net.WebRequest.Create uri
            req.Timeout <- 60 * 60 * 1000 // timeout = 1 hour
            let resp:System.Net.WebResponse = req.GetResponse ()
            resp.GetResponseStream ()
        load formatInfo streamGen
        
    /// Create a tap from a file, guessing the format based on the file extension.
    let public readFromFileAuto (fileName:string) = readFromFile (formatFromFileName fileName) fileName
        
    /// Similar to loadCsvFromFile, only the input is a string not a file 
    let public readFromString (formatInfo:FormatInfo) (str:string) =
        load formatInfo (fun () -> new System.IO.MemoryStream (System.Text.Encoding.Default.GetBytes(str)) :> System.IO.Stream)
        
    /// Use to create a tap from a multi-file binary format. The multi-file format is an 
    /// efficient column storage machanism. 
    let public readFromMultiFile path =
        let tap () = Internal.BinarySerialization.Deserialize.deserializeTupleSequenceAtDir path
        tap |> Tap.Create
        
                                          
    /// Create a tap from a database table                     
    let public readFromDb (db,tableName) =
        let tt = Internal.DbIO.TupleTable.Create(db,tableName,Internal.DbIO.TableReadOnly)
        tt.Read |> Tap.Create
    
    let private write name (formatInfo:FormatInfo) streamFun : ISink =
        let streamLazy = lazy (streamFun ())
        let sinkFun tseq=  
            let stream = streamLazy.Value
            formatInfo.writeSeq stream tseq
            stream.Close ()
        Sink.Create(name, sinkFun)
    
    /// Returns a sink that writes to mutli-file binary column storage            
    let public writeMultiFile path =        
        let sinkFun tseq =            
            Internal.BinarySerialization.Serialize.serializeTupleSequenceToDir(path, tseq, Internal.BinarySerialization.NoCompression)
        Sink.Create(path, sinkFun)
        
    /// Returns a sink that writes to a database table. If the table
    /// already exists, the table will be dropped first. 
    let public writeDb (db,tableName) =
        let table = Internal.DbIO.TupleTable.Create(db,tableName,Internal.DbIO.TableCreate)
        let sinkFun tseq =            
            table.Write tseq
            table.Close ()
        Sink.Create(tableName, sinkFun)
        
    /// Returns a sink that appends to a database table. 
    let public appendDb (db,tableName) =
        let table = Internal.DbIO.TupleTable.Create(db,tableName,Internal.DbIO.TableAppend)
        let sinkFun tseq =            
            table.Write tseq
            table.Close ()
        Sink.Create(tableName, sinkFun)
                    
    /// Returns a sink that writes an already-opened stream 
    let public writeToStream (formatInfo:FormatInfo) stream : ISink =
        write (sprintf "%A" stream) formatInfo (fun () -> stream)
        
    /// Returns a Sink that writes tuples to a named file. 
    let public writeToFile (formatInfo:FormatInfo) (fileName:string) : ISink =
        let loose = match fileName with
                    | "-" -> (fun () -> System.Console.OpenStandardOutput())
                    | _ -> let dir = match System.IO.Path.GetDirectoryName(fileName) with
                                              | "" -> "."
                                              | x -> x
                           System.IO.Directory.CreateDirectory(dir) |> ignore
                           (fun () -> new System.IO.FileStream(fileName, System.IO.FileMode.Create, System.IO.FileAccess.Write,
                                                        System.IO.FileShare.Read,
                                                        0x10000) :> System.IO.Stream)
        write fileName formatInfo loose 
        
    /// Create a tap from a file, guessing the format based on the file extension.
    let public writeToFileAuto (fileName:string) = writeToFile (formatFromFileName fileName) fileName
     
    /// Like writeToFile only returns a sink that writes to a string. 
    let public writeToString (formatInfo:FormatInfo) : ISink =
        write "WriteToString" formatInfo (fun () -> new System.IO.MemoryStream () :> System.IO.Stream)
