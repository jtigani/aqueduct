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

open System
open System.IO

open Aqueduct.Primitives

module BinarySerialization =

    module private Util =

        let public constf k _ = k

        let public flip f x y = f y x

        let public (<<<) f g x = (use y = g x in f y)
        let public (>>>) g f   = f <<< g

        let public numbered  xs = List.zip  [0 .. List.length xs - 1] xs
        let public numbered2 xs = List.zip3 [0 .. List.length xs - 1] xs

        let public makeCounter() =
            let n = ref 0 in fun() ->
                let i = !n in (n := i + 1; i)
                
        let public magic = 0xdeadbeeff00df00dUL

        module public List' =
            let rec public iter3 f xs ys zs =
                match (xs, ys, zs) with
                | (x'::xs', y'::ys', z'::zs') -> f x' y' z'; iter3 f xs' ys' zs'
                | (_, _, _) -> ()
            let rec public whileSome f = function
                | [] -> []
                | x :: xs -> (match f x with None -> [] | Some x' -> x' :: whileSome f xs)

        module public Seq' =
            let rec public whileSome f = Seq.map f >> Seq.takeWhile (function Some _ -> true | None -> false) >> Seq.choose id
            let public forever = seq { while true do yield () }

        module public Array' =
            let public partition m xs =
                let n  = Array.length xs
                let n' = n / m
                if  n' * m <> n then raise (new ArgumentException("Array'.partition: indivisible", "p"))
                Array.init n' (fun j -> let i0 = j * m in [| for i in i0 .. i0 + m - 1 do yield xs.[i] |])

    open Util

    module private EncodeDecode =
        module public Int =
            let public to_bytes(i : int64) = BitConverter.GetBytes(i)
            let public of_bytes(bs : byte[], idx) : int64 = BitConverter.ToInt64(bs, idx)
        module public UInt =
            let public to_bytes(i : uint64) = BitConverter.GetBytes(i)
            let public of_bytes(bs : byte[], idx) : uint64 = BitConverter.ToUInt64(bs, idx)
        module public Float =
            let public to_bytes(f : double) = BitConverter.GetBytes(f)
            let public of_bytes(bs : byte[], idx) : double = BitConverter.ToDouble(bs, idx)
        module public String =
            let public to_byte_count_bytes(s : string) : byte[] = s |> System.Text.Encoding.UTF8.GetByteCount |> BitConverter.GetBytes
            let public to_bytes(s : string) = System.Text.Encoding.UTF8.GetBytes(s)
            let public of_bytes(bs : byte[], idx, len) = System.Text.Encoding.UTF8.GetString(bs, idx, len)
            let public byte_count_size = sizeof<int32>
            let public count_of_count_bytes(bs : byte[], idx) = BitConverter.ToInt32(bs, idx)

    type public Compression = NoCompression | Compressed

    /// This module is supposed to be private; we're forced to make it public by one of F#'s many
    /// annoying and arbitrary-seeming restrictions ("NO PRIVATE TYPE ALIASES DAMMIT!!!").
    module Indices =
        type public SeqIdx = int64
        let  public seqidx = int64
    open Indices

    module Serialize =

        let private openFile (path : string) (cmpn : Compression) =
            let innerStream = new FileStream(path, System.IO.FileMode.Create,
                                                   System.IO.FileAccess.Write,
                                                   System.IO.FileShare.Read,
                                                   0x40000)
            match cmpn with
            | NoCompression -> innerStream :> System.IO.Stream
            | Compressed ->
                let mode = System.IO.Compression.CompressionMode.Compress
                new System.IO.Compression.GZipStream(innerStream, mode) :> System.IO.Stream

        type SequenceSerialization = SeqSer of ColumnSerialization list with
            interface IDisposable with
                member this.Dispose() = for colSer in (match this with SeqSer cs -> cs) do (colSer :> IDisposable).Dispose()
            member public this.Columns = match this with SeqSer cs -> cs
        and ColumnSerialization = AtomColSer of Out | SeqColSer of SeqIdxOut * SequenceSerialization with
            interface IDisposable with
                member this.Dispose() =
                    match this with
                    | AtomColSer out             -> (out       :> IDisposable).Dispose()
                    |  SeqColSer(out, subseqSer) -> (out       :> IDisposable).Dispose()
                                                    (subseqSer :> IDisposable).Dispose()
        and Out = Out of (byte[] -> unit) * (unit -> unit) with
            interface IDisposable with member this.Dispose() = match this with Out(_, finish) -> finish()
            member public this.Write = match this with Out(write, _) -> write
            member public this.Close = match this with Out(_, close) -> close
            static member public Null = Out((fun _ -> ()), (fun() -> ()))
            /// Opens the file at 'path' and produces an 'Out' that writes to it. Closes and disposes of the file during cleanup.
            static member public ToFile (cmpn : Compression) (path : string) =
                let file = openFile path cmpn
                let isOpen = ref true
                let write(bs : byte[]) = file.Write(bs, 0, Array.length bs)
                let close() = if !isOpen then (file.Flush(); file.Close(); (file :> IDisposable).Dispose(); isOpen := false)
                Out(write, close)
            /// The 'Out' produced by this method will never close or dispose of 'stream'.
            /// It's up to the caller to do those things, if necessary.
            static member public ToStream(stream : Stream) =
                let stillWriting = ref true
                let write(bs : byte[]) = if !stillWriting then stream.Write(bs, 0, Array.length bs)
                let finish() = if !stillWriting then (stream.Flush(); stillWriting := false)
                Out(write, finish)
            static member public Daisychain(length : int, dest : Stream) : Out list =
                if length < 1 then [] else
                let bufs   = Array.init   length (fun _ -> new MemoryStream())
                let isDone = Array.create length           false
                let lastDumped = ref(-1)
                let tryDumpNext() =
                    let last = !lastDumped
                    if  last = length - 1 then false else
                    let curr = last + 1
                    if  not(isDone.[curr]) then false else
                    let buf = bufs.[curr]
                    buf.Flush()
                    buf.Close()
                    buf.WriteTo(dest)
                    buf.Dispose()
                    lastDumped := curr
                    true
                List.init length (fun i ->
                    let buf = bufs.[i]
                    let write(bs : byte[]) = if not(isDone.[i]) then buf.Write(bs, 0, Array.length bs)
                    let finish() = (isDone.[i] <- true; while tryDumpNext() do ())
                    Out(write, finish))
        and SeqIdxOut = private {out : Out; latest : SeqIdx ref} with
            interface IDisposable with member this.Dispose() = (this.out :> IDisposable).Dispose()
            member public this.Latest = !this.latest
            member public this.Write(idx : SeqIdx) =
                this.out.Write(idx |> EncodeDecode.Int.to_bytes)
                this.latest := idx
            member public this.Close() = this.out.Close()
            static member public To(out : Out) : SeqIdxOut = {out = out; latest = ref(seqidx 0)}

        let rec public serializeSchemaToFile(path : string, sch : Schema, cmpn : Compression) : unit =
            use file = openFile path cmpn
            serializeSchemaToStream file sch
            file.Flush()
            file.Close()

        and public serializeSchemaToStream (stream : Stream) (sch : Schema) : unit =
            serializeSchemaWith (new System.IO.BinaryWriter(stream)) sch

        and private serializeSchemaOut (out : Out) (sch : Schema) : unit =
            use buf = new MemoryStream()
            serializeSchemaToStream buf sch
            buf.Flush()
            buf.Close()
            buf.ToArray() |> out.Write

        and private serializeSchemaWith (encoder : BinaryWriter) (schema : Schema) =
            let writeOneComp(comp : Column) =
                encoder.Write comp.name
                match comp.element with
                | IntElement -> encoder.Write('I')
                | UnsignedElement -> encoder.Write('U')
                | FloatElement -> encoder.Write('F')
                | StringElement -> encoder.Write('S')
                | SequenceElement innerSch ->
                    encoder.Write('T')
                    serializeSchemaWith encoder innerSch
                serializeDatumWith encoder comp comp.def
            let writeOneOrd(ord : Ordering) =
                encoder.Write (snd ord)
                match fst ord with
                | Ascending -> encoder.Write 'A'
                | Descending -> encoder.Write 'D'
                | Grouped -> encoder.Write 'G'
                | Unordered -> failwith "unexpected ordering in key"
            encoder.Write(schema.Columns |> List.length)
            List.iter writeOneComp schema.Columns
            encoder.Write(schema.Key.isUnique)
            encoder.Write(schema.Key.orderings |> List.length)
            List.iter writeOneOrd schema.Key.orderings

        and private serializeDatumWith (encoder : BinaryWriter) (comp : Column) (value : Datum) =
            match comp.element with
            | IntElement -> value.asInt |> encoder.Write
            | UnsignedElement -> value.asUnsigned |> encoder.Write
            | FloatElement -> value.asFloat |> encoder.Write
            | StringElement -> value.asString |> encoder.Write
            | SequenceElement _ ->
                let tseq = value.asTuples
                let tlist = tseq |> List.ofSeq
                tlist |> List.length |> encoder.Write
                for tup in tlist do serializeTupleWith encoder tup

        and public serializeTupleToStream (stream : Stream) = serializeTupleWith (new System.IO.BinaryWriter(stream))

        and private serializeTupleWith (encoder : BinaryWriter) (tuple : Tuple) =
            List.iter2 (serializeDatumWith encoder) tuple.schema.Columns tuple.values
        
        let private writeMagic stream = 
            let writer = new System.IO.BinaryWriter (stream)
            writer.Write(Util.magic)
            
        let public serializeTupleSequenceToStream (stream : Stream) (tseq : TupleSequence) : unit =
            writeMagic stream
            tseq.Schema |> serializeSchemaToStream stream
            tseq |> Seq.iter (serializeTupleToStream stream)

        // Everything after this point in the module is dedicated to the multi-file, column-based serialization mode:

        let rec private initTupSeqSer(path : string, sch : Schema, cmpn : Compression) : SequenceSerialization =
            if Directory.Exists(path) then Directory.Delete(path, true)
            let seqDir = Directory.CreateDirectory(path)
            let filename(name : string) = Path.Combine(path, name)
            let outFile = (Out.ToFile cmpn) << filename
            let idxFile = SeqIdxOut.To << outFile
            serializeSchemaToFile(filename "schema.sch", sch, cmpn)
            SeqSer [
                for col in sch.Columns do
                    let idxFileName = col.name + ".idx"
                    let colFileName = col.name + ".col"
                    match col.element with
                    | SequenceElement sch' ->
                        yield SeqColSer(idxFile idxFileName, initTupSeqSer(filename colFileName, sch', cmpn))
                    | ty -> yield AtomColSer(outFile colFileName) ]

        let rec private doTupSeqSer(seqSer : SequenceSerialization, idx : SeqIdx, ts : TupleSequence) : SeqIdx =
            let fold f = Seq.fold f idx ts in fold (fun idx t -> // Requirement: Consume 'ts' only once! (This is where we're doing it.)
                let doOne (colTy : Column) colSer (datum:Datum) =
                    match colTy.element with
                    | SequenceElement _ ->
                        match colSer with
                        | SeqColSer(idxOut, seqSer') -> doTupSeqSer(seqSer', idxOut.Latest, datum.asTuples) |> idxOut.Write
                        | _ -> failwith "column type mismatch"
                    | ty -> match colSer with
                            | AtomColSer colOut ->
                                match ty with
                                | IntElement -> datum.asInt |> EncodeDecode.Int.to_bytes |> colOut.Write
                                | UnsignedElement -> datum.asUnsigned |> EncodeDecode.UInt.to_bytes |> colOut.Write
                                | FloatElement -> datum.asFloat |> EncodeDecode.Float.to_bytes |> colOut.Write
                                | StringElement ->
                                    let dtmStr = datum.asString
                                    dtmStr |> EncodeDecode.String.to_byte_count_bytes |> colOut.Write
                                    dtmStr |> EncodeDecode.String.to_bytes            |> colOut.Write
                                | SequenceElement _ -> failwith "column type mismatch"
                            | _                  -> failwith "column type mismatch"
                List'.iter3 doOne ts.Schema.Columns seqSer.Columns t.values
                idx + (seqidx 1))

        let public serializeTupleSequenceToDir(path : string, tseq : TupleSequence, cmpn : Compression) : unit =
            use seqSer = initTupSeqSer(path, tseq.schema, cmpn) in doTupSeqSer(seqSer, seqidx 0, tseq) |> ignore

    module Deserialize =

        let private openFile (path : string) (cmpn : Compression) =
            let innerStream = new FileStream(path, System.IO.FileMode.Open,
                                                   System.IO.FileAccess.Read,
                                                   System.IO.FileShare.ReadWrite,
                                                   0x40000)
            match cmpn with
            | NoCompression -> innerStream :> System.IO.Stream
            | Compressed ->
                let mode = System.IO.Compression.CompressionMode.Decompress
                new System.IO.Compression.GZipStream(innerStream, mode) :> System.IO.Stream

        exception public BadInput of string
        exception private EndOfInput

        type SequenceDeserialization = SeqDeser of Schema * ColumnDeserialization list with
            interface IDisposable with
                member this.Dispose() = for colDeser in (match this with SeqDeser(_, ds) -> ds) do (colDeser :> IDisposable).Dispose()
            member public this.Schema  = match this with SeqDeser(sch, _) -> sch
            member public this.Columns = match this with SeqDeser(_, cs) -> cs
        and ColumnDeserialization = AtomColDeser of In | SeqColDeser of SeqIdxIn * SequenceDeserialization with
            interface IDisposable with
                member this.Dispose() =
                    match this with
                    | AtomColDeser input               -> (input       :> IDisposable).Dispose()
                    |  SeqColDeser(input, subseqDeser) -> (input       :> IDisposable).Dispose()
                                                          (subseqDeser :> IDisposable).Dispose()
        and In = In of (int -> (byte[] * int)) * (unit -> unit) with
            interface IDisposable with member this.Dispose() = match this with In(_, finish) -> finish()
            member public this.Read(count : int) = match this with In(read, _) -> read(count)
            member public this.Finish() = match this with In(_, finish) -> finish()
            static member public FromFile (cmpn : Compression) (path : string) =
                let file = openFile path cmpn
                let isOpen = ref true
                let bufferSize = 0x100000
                let newBuffer n = Array.create (max n bufferSize) (byte 0)
                let buffer = ref( Array.create 0 (byte 0))
                let currentIdx = ref 0
                let endIdx = ref 0
                let readFromFile n =
                    let validLen = !endIdx - !currentIdx
                    let nextBuffer  = newBuffer n
                    System.Array.Copy(!buffer, !currentIdx, nextBuffer, 0, validLen)
                    let readRequestCount = Array.length nextBuffer - validLen
                    let readCount = file.Read(nextBuffer, validLen, readRequestCount)
                    if readCount = 0 then (if validLen <> 0 then raise (BadInput "end of stream unexpected") else raise EndOfInput)
                    buffer := nextBuffer
                    currentIdx := 0
                    endIdx := validLen + readCount
                let read n  =
                    if n + !currentIdx > !endIdx then readFromFile n
                    let idx = !currentIdx
                    currentIdx := idx + n
                    (!buffer, idx)
                let close() =
                    if !isOpen then (file.Close(); (file :> IDisposable).Dispose(); isOpen := false)
                In(read, close)
        and SeqIdxIn = private {input : In; latest : SeqIdx ref} with
            interface IDisposable with member this.Dispose() = (this.input :> IDisposable).Dispose()
            member public this.Latest = !this.latest
            member public this.Read() =
                let idx = AtomReader.ReadInt(this.input)
                this.latest := idx
                idx
            member public this.Close() = this.input.Finish()
            static member public From(input : In) = {input = input; latest = 0 |> seqidx |> ref}

        /// This class is a module in spirit, but the 'and' syntax won't let it actually be one! Yay ML syntax!
        and private AtomReader =
            static member public ReadInt(input : In) = input.Read(sizeof<int64>) |> EncodeDecode.Int.of_bytes
            static member public ReadUInt(input : In) = input.Read(sizeof<uint64>) |> EncodeDecode.UInt.of_bytes
            static member public ReadFloat(input : In) = input.Read(sizeof<double>) |> EncodeDecode.Float.of_bytes
            static member public ReadString(input : In) =
                let size = input.Read(EncodeDecode.String.byte_count_size) |> EncodeDecode.String.count_of_count_bytes
                let (bytes, idx) = input.Read(size)
                EncodeDecode.String.of_bytes(bytes, idx, size)

        /// This is supposed to be part of AtomReader. It's detached because, unlike the other half, it
        /// really crucially *needs* to be a module and it can afford to be one because it lacks the other
        /// chunk's mutual dependence on the In class. Ideally the two halves would be unified and the
        /// one true AtomReader would be a module, but alack, the current configuration is how they must
        /// dwell. Our sad tale has a moral: THE 'and' SYNTAX IS A FUCKING MESS. Thank you and good night.
        module private AtomReader' =
            let private assertLength want got =
                if got = want then () else
                if got = 0 then raise EndOfInput else
                raise <| BadInput "unexpected length"
            let private readExactAmount(amount : int, stream : Stream, buffer : byte[]) =
                stream.Read(buffer, 0, amount) |> assertLength amount
            let public readIntFromStream =
                let buf = Array.create sizeof<int64> (byte 0)
                fun(stream : Stream) ->
                    readExactAmount(sizeof<int64>, stream, buf)
                    EncodeDecode.Int.of_bytes(buf, 0)
            let public readUIntFromStream =
                let buf = Array.create sizeof<uint64> (byte 0)
                fun(stream : Stream) ->
                    readExactAmount(sizeof<uint64>, stream, buf)
                    EncodeDecode.UInt.of_bytes(buf, 0)
            let public readFloatFromStream =
                let buf = Array.create sizeof<double> (byte 0)
                fun(stream : Stream) ->
                    readExactAmount(sizeof<double>, stream, buf)
                    EncodeDecode.Float.of_bytes(buf, 0)
            let public readStringFromStream =
                let szBuf = Array.create EncodeDecode.String.byte_count_size (byte 0)
                fun(stream : Stream) ->
                    let size =
                        readExactAmount(EncodeDecode.String.byte_count_size, stream, szBuf)
                        EncodeDecode.String.count_of_count_bytes(szBuf, 0)
                    let strBuf = Array.create size (byte 0)
                    readExactAmount(size, stream, strBuf)
                    EncodeDecode.String.of_bytes(strBuf, 0, size)

        type private ReadLimit = Limit of SeqIdx | Unlimited with
            member public this.Range =
                let (peek, bump) =
                    let i = ref(seqidx 0)
                    let peek() = !i
                    let bump() = (let p = peek() in (i := p + (seqidx 1); p))
                    in (peek, bump)
                match this with
                | Limit n -> Seq.init (int n) seqidx
                | Unlimited -> seq { while true do yield bump() }

        let rec public deserializeSchemaFromFile(path : string) : Schema =
            use file = openFile path (if path.Trim().EndsWith(".gz", StringComparison.CurrentCultureIgnoreCase) then Compressed else NoCompression)
            let sch = deserializeSchemaFromStream file
            file.Close()
            sch

        and public deserializeSchemaFromStream(stream : Stream) = deserializeSchemaWith(new System.IO.BinaryReader(stream))

        and private deserializeSchemaWith(decoder : BinaryReader) =
            let readOneComp () =
                let name = decoder.ReadString ()
                let elCh = decoder.ReadChar ()
                let el = match elCh with
                         | 'I' -> IntElement
                         | 'U' -> UnsignedElement
                         | 'F' -> FloatElement
                         | 'S' -> StringElement
                         | 'T' -> SequenceElement (deserializeSchemaWith decoder)
                         | _ -> failwith "unexpected char in schema stream"
                let def = deserializeDatumWith decoder el
                Column.Create(name, el, def)
            let readOneOrd () =
                let fld = decoder.ReadString ()
                let ordCh = decoder.ReadChar ()
                let ord = match ordCh with
                          | 'A' -> Ascending
                          | 'D' -> Descending
                          | 'G' -> Grouped
                          | _ -> failwith "unexpected  char in schema stream"
                in (ord, fld)
            let fldCount = decoder.ReadInt32 ()
            let comps = [for ii in 1..fldCount do yield readOneComp ()]
            let isUnique = decoder.ReadBoolean ()
            let keyCount = decoder.ReadInt32 ()
            let ords = [for ii in 1 .. keyCount do yield readOneOrd ()]
            let key = SchemaKey.Create(ords,isUnique)
            Schema.Create(comps, key)

        and private deserializeDatumWith (decoder : BinaryReader) element =
            match element with
                | IntElement -> decoder.ReadInt64 () |> Datum.Int
                | UnsignedElement -> decoder.ReadUInt64 () |> Datum.Unsigned
                | FloatElement -> decoder.ReadDouble () |> Datum.Float
                | StringElement -> decoder.ReadString () |> Datum.String
                | SequenceElement innerSch ->
                    let count = decoder.ReadInt32()
                    let tseq = seq [for ii in 1..count do yield deserializeTupleWith decoder innerSch]
                    TupleSequence.Create innerSch tseq |> Datum.Tuples

        and public deserializeTupleFromStream (stream : Stream) = deserializeTupleWith (new System.IO.BinaryReader(stream))

        and private deserializeTupleWith (decoder : System.IO.BinaryReader) (sch : Schema) =
            let values = sch.Columns  |> List.map (fun comp -> deserializeDatumWith decoder comp.element)
            Tuple.Create sch values

        let private checkMagic stream =
            let decoder = new System.IO.BinaryReader (stream)
            let token = decoder.ReadUInt64 ()
            if token <> Util.magic then raise (BadInput (sprintf "magic token %x missing. Got %x" Util.magic token))
        let public deserializeTupleSequenceFromStream(stream : Stream) : TupleSequence =
            checkMagic stream
            let sch = deserializeSchemaFromStream stream
            Seq'.forever |> Seq'.whileSome (fun _ ->
                if not stream.CanRead then None else
                try deserializeTupleFromStream stream sch |> Some
                with :? EndOfStreamException              -> None) |>
            TupleSequence.CreateUnsafe sch

        // Everything after this point in the module is dedicated to the multi-file, column-based serialization mode:

        let rec private initTupSeqDeser (cmpn : Compression) (path : string) : SequenceDeserialization =
            let cmpnExtn = match cmpn with NoCompression -> "" | Compressed -> ".gz"
            let sch = deserializeSchemaFromFile(Path.Combine(path, "schema.sch" + cmpnExtn))
            let cols = [
                for col in sch.Columns do
                    match col.element with
                    | IntElement
                    | UnsignedElement
                    | FloatElement
                    | StringElement  -> yield AtomColDeser(Path.Combine(path, col.name + ".col" + cmpnExtn) |> In.FromFile cmpn)
                    | SequenceElement _ -> yield  SeqColDeser(Path.Combine(path, col.name + ".idx" + cmpnExtn) |> In.FromFile cmpn |> SeqIdxIn.From,
                                                           Path.Combine(path, col.name + ".col" + cmpnExtn) |> initTupSeqDeser cmpn) ]
            SeqDeser(sch, cols)

        let rec private doTupSeqDeser(seqDeser : SequenceDeserialization, limit : ReadLimit) =
            let inline decodeAtomCol input element =
                match element with
                |      IntElement -> AtomReader.ReadInt    input |> Datum.Int
                | UnsignedElement -> AtomReader.ReadUInt   input |> Datum.Unsigned
                |    FloatElement -> AtomReader.ReadFloat  input |> Datum.Float
                |   StringElement -> AtomReader.ReadString input |> Datum.String
                |               _ -> failwith "type mismatch"
            let inline decodeSeqCol (idxIn:SeqIdxIn) subseqDeser (element:SchemaElement) =
                let sch' = match element with SequenceElement sch -> sch | _ -> failwith "bad"
                let subseqLimit = Limit(let lastLim = idxIn.Latest in idxIn.Read() - lastLim)
                let innerSeq = doTupSeqDeser(subseqDeser, subseqLimit) |> List.ofSeq |> Seq.ofList
                innerSeq |> TupleSequence.Create sch' |> Datum.Tuples
            let  decodeColumn (col:Column) deser =
                match deser with
                | AtomColDeser input -> decodeAtomCol input col.element                                
                | SeqColDeser(idxIn, subseqDeser) -> decodeSeqCol idxIn subseqDeser col.element
                
            limit.Range |> Seq'.whileSome (fun _ ->
                try Some <| Tuple.Create seqDeser.Schema
                        (List.map2 decodeColumn seqDeser.Schema.Columns seqDeser.Columns)
                with EndOfInput -> None)

        let public deserializeTupleSequenceAtDir(path : string) : TupleSequence =
            let (sch, cmpn) =
                let schPath = Path.Combine(path, "schema.sch")
                if File.Exists(schPath) then (deserializeSchemaFromFile schPath, NoCompression) else
                let schPath = Path.Combine(path, "schema.sch.gz")
                if File.Exists(schPath) then (deserializeSchemaFromFile schPath, Compressed) else
                failwith "schema not found"
            TupleSequence.Create sch <| seq {
                use seqDeser = initTupSeqDeser cmpn path
                yield! doTupSeqDeser(seqDeser, Unlimited) }