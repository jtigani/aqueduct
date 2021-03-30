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

namespace MindsetMedia.Metrics

open Xunit


module Test=
    open Aqueduct
    open Aqueduct.Internal
    open Aqueduct.Primitives
    open Aqueduct.Common
    open Aqueduct.Pipes
    open Aqueduct.Join
    open Aqueduct.IO
    open Aqueduct.MetricsDb
    
    open Aqueduct.Test.Utils
    
    let String = Datum.String
    let Int = Datum.Int
    let Float = Datum.Float
    let Tuples = Datum.Tuples
    
    let TupAddColumn (tup:Tuple) name (datum:Datum) = Tuple.Create (tup.schema.Add (Column.Make(name, datum.Typeof))) (datum :: tup.values)
        
    let TupDropColumns (x:Tuple) names =
        let skipIndices = names |> List.map (fun n -> x.schema.NameIndex n) |> Set.ofList
        let valueEnum = (x.values |> Seq.ofList).GetEnumerator ()
        let max = (List.length x.values) - 1
        let values =
            [for i in 0.. max do
                valueEnum.MoveNext() |> ignore
                if (not <| Set.contains i skipIndices) then yield valueEnum.Current
            ]
        let schema = x.schema.DropColumns names
        Tuple.Create schema values
        
    let testSchemaCreate a b = List.zip b a |> Schema.Make
    let testSchemaCreateWithDefaultValues a b c = List.zip3 c a b |> List.map (fun (x,y,z) -> Column.Create (x,y,z)) |> Schema.Create
    let schema3 a b c= List.zip3 a b c |> List.map (fun (x,y,z) -> Column.Create (x,y,z)) |> Schema.Create
    let sc2 (x,y) = Column.Make (x,y) 
    let sc3 (x,y,z) = Column.Create (x,y,z)     
    let dbfactory () = System.Data.SQLite.SQLiteFactory () :>  System.Data.Common.DbProviderFactory
    
    type SetUpDbAttribute() =         
        inherit Xunit.BeforeAfterTestAttribute()
        override x.Before(_) =           
            Aqueduct.MetricsDb.SetMetricsDb (Aqueduct.Database.DbData.CreateWithFactory(Aqueduct.Database.Sqlite,"Data Source=metrics.db;Version=3;",dbfactory()))
            
                    
    let TestRunPipe (name,tap) pipestages =
        let sink = new CollectorSink ()        
        RunSimplePipe sink pipestages (name,tap) 
        sink.GetSeq         
   
            
    let ExportToStrings (x:Tuple) = x.values |> List.map (fun d -> d.ToString()) 

    [<Fact>]
    let test1 () = 
        let assertSplit splitter str expected =
            let input:Tuple = Tuple.CreateFromStrings [str]
            let mapper = splitter 
            let res = input |> mapper |> ExportToStrings 
            assertEqual expected res
            
        let splitter n = Aqueduct.Common.lineSplitterMap(n,',')
        assertSplit (splitter 1 )"" [""]
        assertSplit (splitter 2)"," ["";""]
        assertSplit (splitter 2)"," ["";""]
        assertSplit (splitter 2)",foo" ["";"foo"]
        assertSplit (splitter 3)",foo," ["";"foo";""]
        assertSplit (splitter 15)",,,,,,,,,,,,,," ["";"";"";"";"";"";"";"";"";"";"";"";"";"";""]
        assertSplit (splitter 2)"foo,bar" ["foo";"bar"]
        assertSplit (splitter 2)"'foo,bar'baz,qux" ["'foo,bar'baz";"qux"]       
        assertSplit (splitter 3)"a,'foo,bar'baz,qux" ["a";"'foo,bar'baz";"qux"]
        assertSplit (splitter 3)"a,'foo,bar'baz,\"qux,\"" ["a";"'foo,bar'baz";"\"qux,\""]
        assertSplit (splitter 4)"a,'foo,bar'baz,\"qux,\",blarg" ["a";"'foo,bar'baz";"\"qux,\"";"blarg"]
        assertSplit (splitter 4)"a,'foo\",\",bar'baz,\"qux,\",blarg" ["a";"'foo\",\",bar'baz";"\"qux,\"";"blarg"]        
        assertSplit (splitter 5) ",'a,b',c\",foo,'\"blah,2,," ["";"'a,b'";"c\"";"foo";"'\"blah,2,,"]
        
        let tsvSplitter n = Aqueduct.Common.lineSplitterMap(n,'\t')
        assertSplit (tsvSplitter 10) @"60229027966	2354.22477510000000000001	1716584	6	4	2009-05-17 15:50:37.146	14805118		6	http://play.clubpenguin.com" 
                                    ["60229027966";"2354.22477510000000000001";"1716584";"6";"4";"2009-05-17 15:50:37.146";"14805118";"";"6";@"http://play.clubpenguin.com"]
                                           

        
    let basicTap tseq = {new ITap with member self.Tap () = tseq}
    
    let assertPipe (pipe:PipeStage) expected (strInput:string list) =
        let input:Tuple list = List.map (fun str -> Tuple.CreateFromStrings [str]) strInput        
        let inseq = input |> Seq.ofList |> TupleSequence.Create (Schema.Make [("$0", StringElement)])
        let tap = basicTap inseq         
        let sink = new CollectorSink()
        RunSimplePipe sink pipe ("tap",tap)                        
        let result = sink.GetSeq.tseq |> Seq.map ExportToStrings |> List.ofSeq
        List.iter2 assertEqual result expected 
        assertEqual expected result
        
            
    [<Fact>]
    let test2 () =                
        let pipe n =
            "tap" 
            *> SplitLineOnComma n
            <*> StripQuotes
        
        assertPipe (pipe 1) [["foo"]] ["foo"]
        assertPipe (pipe 2) [["foo";""]] ["foo,"]
        assertPipe (pipe 2) [["a";"b"] ; ["c";"d"] ; ["e";""]] ["a,b";"c,d";"e"] 
        assertPipe (pipe 4) [["a";"b";"c";""] ; ["c";"d";"e,f";"g"] ; ["h";"";"";""]] ["a,b,c";"c,'d',\"e,f\",g";"h"] 
        assertPipe (pipe 0) [] []
        ()
        
    [<SetUpDb>]
    [<Fact>]
    let testDbIO1 () = 
                    
        let tseqCheck tableName (tseq:TupleSequence) =             
            let db = Aqueduct.Database.DbData.CreateWithFactory(Aqueduct.Database.Sqlite,"Data Source=dbIoTest.db;Create=true;Version=3;",dbfactory())
            let tableW = DbIO.TupleTable.Create(db,tableName,DbIO.TableCreate)
            let tableR = DbIO.TupleTable.Create(db,tableName,DbIO.TableReadOnly,tseq.Schema)
            let tableRW = DbIO.TupleTable.Create(db,tableName,DbIO.TableAppend,tseq.Schema)            
            
            tableW.Write(tseq)
            let read1 = tableW.Read ()            
            assertTseqEq tseq read1
            
            let read2 = tableR.Read ()            
            assertTseqEq tseq read2
            
            let read3 = tableRW.Read ()
            assertTseqEq tseq read3
            
            let tableI = DbIO.TupleTable.Create(db,tableName,DbIO.TableReadOnly)
            let read4 = tableI.Read ()
            let tseq' = 
                if tseq.Schema.Key.isUnique = false then 
                    TupleSequence.CreateKeyed (tseq.Schema.SetKey SchemaKey.Empty) tseq.tseq
                else tseq
            assertTseqEq tseq' read4 
            
        let schema = Schema.Make [("A", IntElement); ("B", StringElement); ("C", FloatElement)]        
        let t1 = Tuple.Create schema [Int 2001L; String "20 01"; Float 2.001]
        let t2 = Tuple.Create schema [Int 2002L; String "20 02"; Float 2.002]
        let tseq1 = TupleSequence.Create schema (seq [t1;t2])
        
        let key1 = SchemaKey.CreateAscending (["A";"B"],false)
        let tseqO1 = TupleSequence.CreateKeyed (schema.SetKey key1) (seq [t1;t2])
        let key2 = SchemaKey.CreateAscending (["C";"A"],true)
        let tseqO2 = TupleSequence.CreateKeyed (schema.SetKey key1) (seq [t1;t2])
        
        let tseqEmpty = TupleSequence.Create schema Seq.empty
                
        tseqCheck "test1" tseq1
        tseqCheck "testEmpty" tseqEmpty
        tseqCheck "testO1" tseqO1
        tseqCheck "testO2" tseqO2
        
    [<SetUpDb>]
    [<Fact>]
    let testDbIO2 () = 
        let schemaI = Schema.Make [("IA", StringElement)]
        let schema = Schema.Make [("A", IntElement); ("B", StringElement); ("C", FloatElement);("D",SequenceElement schemaI)]        
        let emptyI = TupleSequence.Create schemaI Seq.empty
        let longStr = 
            let builder = new System.Text.StringBuilder ()
            for ii in 0..0x1000 do builder.Append("abcdefghijklmnopqrstuvwxyz ") |> ignore
            builder.ToString ()
            
        let t1I = Tuple.Create schemaI [String longStr]
        let nonEmptyI = TupleSequence.Create schemaI (seq [t1I])
        let t1 = Tuple.Create schema [Int 2001L; String "20 01"; Float 2.001; Tuples emptyI]
        let t2 = Tuple.Create schema [Int 2002L; String "20 02"; Float 2.002; Tuples nonEmptyI]        
        let tseq1 = TupleSequence.Create schema (seq [t1;t2])
        let tseqEmpty = TupleSequence.Create schema Seq.empty
        
        let tseqCheck tseq = 
            let tableName = System.Guid.NewGuid ()
            let db = Aqueduct.Database.DbData.CreateWithFactory(Aqueduct.Database.Sqlite,"Data Source=dbIoTest.db;Create=true;Version=3;",dbfactory())
            let tableW = DbIO.TupleTable.Create(db,"test1",DbIO.TableCreate)
            let tableR = DbIO.TupleTable.Create(db,"test1",DbIO.TableReadOnly,schema)
            let tableRW = DbIO.TupleTable.Create(db,"test1",DbIO.TableAppend,schema)
            
            tableW.Write(tseq)
            let read1 = tableW.Read ()            
            assertTseqEq tseq read1
            let read2 = tableR.Read ()
            
            assertTseqEq tseq read2
            let read3 = tableRW.Read ()
            assertTseqEq tseq read3
        tseqCheck tseq1
        tseqCheck tseqEmpty

    [<SetUpDb>]
    [<Fact>]
    let testDbEndToEndIO () =         
        let schema = Schema.Make [("A", IntElement); ("B", StringElement); ("C", FloatElement)]        
        let t1 = Tuple.Create schema [Int 2001L; String "20 01"; Float 2.001]
        let t2 = Tuple.Create schema [Int 2002L; String "20 02"; Float 2.002]
        let key = SchemaKey.Create ([Ascending,"A"],true)
        let tseq = TupleSequence.Create schema (seq [t1;t2])        
        let tableNameIn = "inTable"
        let tableNameOut = "outTable"
        let dbIn = Aqueduct.Database.DbData.CreateWithFactory(Aqueduct.Database.Sqlite,"Data Source=dbIoTestIn.db;Create=true;Version=3;",dbfactory())
        let dbOut = Aqueduct.Database.DbData.CreateWithFactory(Aqueduct.Database.Sqlite,"Data Source=dbIoTestOut.db;Create=true;Version=3;",dbfactory())
        let tableW = DbIO.TupleTable.Create(dbIn,tableNameIn,DbIO.TableCreate)
        tableW.Write(tseq)
        
        let tap = IO.readFromDb(dbIn,tableNameIn)
        let sink= IO.writeDb(dbOut,tableNameOut)
        
        let pipes = "tap" *> IdentityPipe 
        Pipes.MultiRunPipes [sink,pipes] ["tap",tap]
        
        let tableR = DbIO.TupleTable.Create(dbOut,tableNameOut,DbIO.TableReadOnly)
        let result = tableR.Read ()
        assertTseqEq tseq result
                                                                        

    [<Fact>]
    let test3 () =                
        let pipe =
            "tap"
            *> SplitLineOnComma 3
            <*> StripQuotes
        
        let csvstr = "blah,blah2,x\r\n'  1  ','2',\"3\"\r\n4,\"5     \",6\r\n7,8,9\r\n10,11,12\r\n13,14,15\r\n16,17,18"        
        let d1 = [ Int 1L ; String "2"; String "3" ]
        let d2 = [ Int 4L ; String "5"; String "6" ]
        let d3 = [ Int 7L ; String "8"; String "9" ]
        let d4 = [ Int 10L ; String "11"; String "12" ]
        let d5 = [ Int 13L ; String "14"; String "15" ]
        let d6 = [ Int 16L ; String "17"; String "18" ]
       
        let schema = Schema.Make [("blah", IntElement); ("blah2", StringElement); ("x", StringElement)]
        
        let t1 = Tuple.Create schema d1
        let t2 = Tuple.Create schema d2
        let t3 = Tuple.Create schema d3
        let t4 = Tuple.Create schema d4
        let t5 = Tuple.Create schema d5
        let t6 = Tuple.Create schema d6
        
        let expected = [t3;t4;t5;t6]
        
        let tap = readFromString (CsvFormatInfo None) csvstr
        let ffun (tup:Tuple) = (tup.GetInt "blah") > 5L
        
        
        let pipe = "tap" *> StripQuotes <*> TrimWhitespace <*> PartialSchemaApply (Schema.Make [("blah", IntElement)]) <*> 
                           SimpleFilter ffun 
        
        let resultVar = (TestRunPipe ("tap",tap) pipe).tseq |> List.ofSeq
        assertEqual schema resultVar.Head.schema
        assertEqual expected resultVar
           
    [<Fact>]
    let test4 () =                
        let pipe =
            "tap" *> StripQuotes  <*> PartialSchemaApply (Schema.Make [("blah", IntElement)]) <*>  DropColumns ["blah2"] <*> AddColumnRegex ("x","as","^a+")
        
        let csvstr = "blah,blah2,x\r\n'  1  ','2',\"aaa3\"\r\n4,\"5     \",6\r\n"        
               
        let e1 = [ String "aaa" ;Int 1L ; String "aaa3"   ]
        let e2 = [ String ""; Int 4L ; String "6"]
                
        let schema = Schema.Make [("as", StringElement); ("blah", IntElement); ("x", StringElement)]
        
        let t1 = Tuple.Create schema e1
        let t2 = Tuple.Create schema e2
        
        
        let expected = [t1;t2] |> Seq.ofList |> TupleSequence.Create schema
        
        let tap = readFromString (CsvFormatInfo None) csvstr        
        let result = TestRunPipe ("tap",tap) pipe 
        

        assertTseqEq expected result
                                                  
        () 
            
    [<Fact>]
    let testRegex () =                
        let pipe =            
                "tap" 
                *> AddColumnsRegex ("url",["domain"; "path"; "queryString"],@"^http://(?<domain>[^/\?]+)(?<path>[^\?]*)(\?*)(?<queryString>(.*))$")
                <*> ToLower ["domain"]
                <*> AddColumnRegex ("domain", "core_domain", @"[^\.]+\.[^\.]+$" )
        
        let url1 = @"http://www.CLASSMATES.com/search/help/index"
        let url2 = @"http://www.ldssingles.com/compass/exam_form.html?submitted=true&;test=readiness"
        let url3 = @"http://foo.com"
        let url4 = @"http://a.b.c.d.co.uk?query1&query2=foo"
                
        let csvstr = sprintf "url\r\n%s\r\n%s\r\n%s\r\n%s" url1 url2 url3 url4
        
               
        let e1 = [ String "classmates.com"; String "www.classmates.com" ; String "/search/help/index"; String ""; String url1 ]
        let e2 = [ String "ldssingles.com"; String "www.ldssingles.com" ; String "/compass/exam_form.html"; String "submitted=true&;test=readiness" ;String url2  ]
        let e3 = [ String "foo.com"; String "foo.com" ; String ""; String "" ; String url3 ]
        let e4 = [ String "co.uk"; String "a.b.c.d.co.uk" ; String ""; String "query1&query2=foo"; String url4 ]
                
        let schema = Schema.Make [("core_domain", StringElement); ("domain", StringElement); ("path", StringElement);
                                    ("queryString", StringElement); ("url", StringElement)]

        
        let t1 = Tuple.Create schema e1
        let t2 = Tuple.Create schema e2
        let t3 = Tuple.Create schema e3
        let t4 = Tuple.Create schema e4
        
        let expected = [t1;t2;t3;t4]
        
        let tap = readFromString (CsvFormatInfo None) csvstr        
        let result = TestRunPipe ("tap",tap) pipe 
        
        let resLst = (result.tseq |> List.ofSeq)
        List.iter2 (fun e r -> assertEqual e.schema r.schema) expected resLst
        List.iter2 (fun e r -> List.iter2 (fun ee rr -> assertEqual ee rr) e.values r.values) expected resLst
        assertEqual expected resLst
        () 
        

    [<Fact>]
    let testSmallJoin () =                
                
        let rs = Schema.Make [("field1", IntElement);("foo", StringElement)]
        let ls = Schema.Make [("field2", FloatElement);("field3", IntElement);("bar", StringElement)]
        let js = Schema.Make [("field2", FloatElement);("field3", IntElement);("bar", StringElement);("field1", IntElement)]
                
        let t1r = Tuple.Create rs [Int 1L; String "match1"]
        let t2r = Tuple.Create rs [Int 2L; String "no-match1"]
        let t3r = Tuple.Create rs [Int 3L; String "match2"]
        
        let t1l = Tuple.Create ls [Float 4.0; Int 4L; String "match2"]
        let t2l = Tuple.Create ls [Float 5.0; Int 5L; String "match1"]
        let t3l = Tuple.Create ls [Float 6.0; Int 6L; String "no-match2"]
        let t4l = Tuple.Create ls [Float 7.0; Int 7L; String "match2"]
                       
        let e1 = Tuple.Create js [Float 4.0; Int 4L; String "match2"; Int 3L] 
        let e2 = Tuple.Create js [Float 5.0; Int 5L; String "match1"; Int 1L] 
        let e3 = Tuple.Create js [Float 7.0; Int 7L; String "match2"; Int 3L] 
        
        let left = [t1l;t2l;t3l;t4l]
        let right = [t1r;t2r;t3r]
        
        let expected = [e1;e2;e3]
        
        let ltap = Tap.Create (fun () -> left |> Seq.ofList |> TupleSequence.Create ls)
        let rtap = Tap.Create (fun () -> right |> Seq.ofList |> TupleSequence.Create rs) 
                
        let rpipe = "right" *> IdentityPipe
            
        let lpipe = "left" *> IdentityPipe <**> (Join (JoinSmall,JoinInner) ["bar"] ["foo"] ,rpipe)
        let taps = ["left",ltap;"right",rtap]
        let sink = new CollectorSink()
        RunPipes sink lpipe taps                
        
        let result = sink.GetSeq.tseq |> List.ofSeq
        
        assertEqual js result.Head.schema
        
        List.iter2 (fun e r -> assertEqual e.schema r.schema) expected result
        List.iter2 (fun e r -> List.iter2 (fun ee rr -> assertEqual ee rr) e.values r.values) expected result
        assertEqual expected result
        () 
        
    [<Fact>]
    let testJoinAsColumn () =                
                
        let rs = Schema.Make [("field1", IntElement);("foo", StringElement)]
        let ls = Schema.Make [("field2", FloatElement);("field3", IntElement);("bar", StringElement)]
        let js = Schema.Make [("field2", FloatElement);("field3", IntElement);("bar", StringElement);("field1", IntElement)]
                
        let t1r = Tuple.Create rs [Int 1L; String "match1"]
        let t2r = Tuple.Create rs [Int 2L; String "no-match1"]
        let t3r = Tuple.Create rs [Int 3L; String "match2"]
        
        let t1l = Tuple.Create ls [Float 4.0; Int 4L; String "match2"]
        let t2l = Tuple.Create ls [Float 5.0; Int 5L; String "match1"]
        let t3l = Tuple.Create ls [Float 6.0; Int 6L; String "no-match2"]
        let t4l = Tuple.Create ls [Float 7.0; Int 7L; String "match2"]
        
        let left = [t1l;t2l;t3l;t4l] |> TupleSequence.Create ls
        let right = [t1r;t2r;t3r] |> TupleSequence.Create rs
        
        let exS = left.Schema.Add (Column.Make("newCol",SequenceElement rs))
        let expected = left.tseq |> Seq.map (fun tup -> TupAddColumn tup "newCol" (Tuples right)) |> TupleSequence.Create exS
                                
        let ltap = Tap.Create (fun () -> left ) 
        let rtap = Tap.Create (fun () -> right) 
                
        let rpipe = "right" *> IdentityPipe            
        let lpipe = "left" *> IdentityPipe <**> (JoinAsColumn "newCol",rpipe)
        let taps = ["left",ltap;"right",rtap]
        let sink = new CollectorSink()
        RunPipes sink lpipe taps
        let result = sink.GetSeq                  
        assertTseqEq expected result        
        () 
        
    [<Fact>]
    let testEmptyJoin () =                
                
        let rs = Schema.Make ["field1",IntElement;"foo",StringElement]
        let ls = Schema.Make ["field2",FloatElement;"field3",IntElement;"bar",StringElement]
        let js = Schema.Make ["field2",FloatElement;"field3",IntElement;"bar",StringElement;"field1",IntElement]

                                                                              
        let expected = TupleSequence.Create js Seq.empty
        
        let ltap = Tap.Create (fun () -> Seq.empty |> TupleSequence.Create ls) 
        let rtap = Tap.Create (fun () -> Seq.empty |> TupleSequence.Create rs) 
                
        let rpipe = "right" *> IdentityPipe
            
        let lpipe = "left" *> IdentityPipe <**> (Join (JoinSmall,JoinInner) ["bar"] ["foo"] ,rpipe)
        let taps = ["left",ltap;"right",rtap]
        let sink = new CollectorSink()
        RunPipes sink lpipe taps                
        
        let result = sink.GetSeq
        assertTseqEq expected result                        
        () 
                        
    let testGroupSize size =
        let sc3 (x,y,z) = Column.Create (x,y,z)
        let ls = Schema.Create [sc3 ("field1",FloatElement,Float -1.0);sc3("key",StringElement,String "def1");sc3 ("key2",IntElement,Int -1L)] 
        let eis = Schema.Create [sc3 ("field1",FloatElement,Float -1.0)] 
        let empty = TupleSequence.Create eis Seq.empty
        let gs = Schema.Create
                    [sc3 ("key",StringElement,String "def1")
                     sc3 ("key2",IntElement,Int -1L)
                     sc3("grouped", SequenceElement eis,Tuples empty)]                
        let t1 = Tuple.Create ls [Float 6.0; String "a";Int 10L]
        let t2 = Tuple.Create ls [Float 5.0; String "a";Int 10L]        
        
        let t3 = Tuple.Create ls [Float 4.0; String "a";Int 20L]        
        
        let t4 = Tuple.Create ls [Float 3.0; String "b";Int 20L]
        
        let t5 = Tuple.Create ls [Float 2.0; String "c";Int 0L]        
        let t6 = Tuple.Create ls [Float 1.0; String "c";Int 0L]        
                                        
        let ei1 = Tuple.Create eis [Float 6.0]
        let ei2 = Tuple.Create eis [Float 5.0]
        let ei3 = Tuple.Create eis [Float 4.0]
        let ei4 = Tuple.Create eis [Float 3.0]
        let ei5 = Tuple.Create eis [Float 2.0]
        let ei6 = Tuple.Create eis [Float 1.0]
        
        let seq1 = TupleSequence.Create eis (seq [ei1;ei2])
        let seq2 = TupleSequence.Create eis (seq [ei3])
        let seq3 = TupleSequence.Create eis (seq [ei4])
        let seq4 = TupleSequence.Create eis (seq [ei5;ei6])
        let e1 = Tuple.Create gs [String "a"; Int 10L; Tuples seq1 ] 
        let e2 = Tuple.Create gs [String "a"; Int 20L; Tuples seq2 ] 
        let e3 = Tuple.Create gs [String "b"; Int 20L; Tuples seq3 ] 
        let e4 = Tuple.Create gs [String "c"; Int 0L; Tuples seq4 ] 
        let exKey = SchemaKey.Create ([Ascending,"key";Ascending,"key2"],true)
        let expected = seq[e1;e2;e3;e4] |> TupleSequence.CreateKeyed (gs.SetKey exKey) 

        
        let input = 
            match size with
            | JoinSmall
            | JoinLarge ->
                TupleSequence.Create   ls ([t1;t2;t3;t4;t5;t6] |> List.rev |> Seq.ofList)                
            | JoinOrdered ->
                let key = exKey.SetUnique(false)
                TupleSequence.CreateKeyed (ls.SetKey key)  (seq [t1;t2;t3;t4;t5;t6])
                    
        let pipes = (Group JoinSmall ["key";"key2"] "grouped")            
        let output = RunPipeFromSeq input pipes
        
        assertTseqEq expected output                
        () 
        
    [<SetUpDb>]          
    [<Fact>]
    let testGroup () =  
        testGroupSize JoinSmall
        testGroupSize JoinOrdered
        testGroupSize JoinLarge
        
    [<Fact>]
    let testSmallOuterJoin () =                
                
        let rs = testSchemaCreate [IntElement;StringElement] ["field1";"foo"]
        let ls = testSchemaCreate [FloatElement;IntElement; StringElement] ["field2";"field3";"bar"]
        let js = testSchemaCreate [FloatElement; IntElement; StringElement; IntElement] ["field2";"field3";"bar";"field1"]                
        let t1r = Tuple.Create rs [Int 1L; String "match1"]
        let t2r = Tuple.Create rs [Int 2L; String "no-match1"]
        let t3r = Tuple.Create rs [Int 3L; String "match2"]
        
        let t1l = Tuple.Create ls [Float 4.0; Int 4L; String "match2"]
        let t2l = Tuple.Create ls [Float 5.0; Int 5L; String "match1"]
        let t3l = Tuple.Create ls [Float 6.0; Int 6L; String "no-match2"]
        let t4l = Tuple.Create ls [Float 7.0; Int 7L; String "match2"]
                       
        let e1 = Tuple.Create js [Float 4.0; Int 4L; String "match2"; Int 3L] 
        let e2 = Tuple.Create js [Float 5.0; Int 5L; String "match1"; Int 1L] 
        let e3 = Tuple.Create js [Float 6.0; Int 6L; String "no-match2"; Int 0L] 
        let e4 = Tuple.Create js [Float 7.0; Int 7L; String "match2"; Int 3L] 
        
        
        let left = [t1l;t2l;t3l;t4l]
        let right = [t1r;t2r;t3r]
        
        let expected = [e1;e2;e3;e4] |> Seq.ofList |> TupleSequence.Create js
        
        let ltap = Tap.Create (fun () -> left |> Seq.ofList |> TupleSequence.Create ls) 
        let rtap = Tap.Create (fun () -> right |> Seq.ofList |> TupleSequence.Create rs)
                
        let rpipe = "right" *> IdentityPipe
            
        let lpipe = "left" *> IdentityPipe <**> (Join (JoinSmall,JoinLeftOuter) ["bar"] ["foo"] ,rpipe)
        let taps = ["left",ltap;"right",rtap]
        let sink = new CollectorSink()
        RunPipes sink lpipe taps                
        let res = sink.GetSeq
        assertTseqEq expected res
        let result = sink.GetSeq.tseq |> List.ofSeq
                
        () 
                    
    let testSerFormat formatter =                
                                
        let d1 = [ Int 1L ; String "2"; String "3" ]
        let d2 = [ Int 4L ; String "5"; String "6" ]
        let d3 = [ Int 7L ; String "8"; String "9" ]
        let d4 = [ Int 10L ; String "11"; String "12" ]
        let d5 = [ Int 13L ; String "14"; String "15" ]
        let d6 = [ Int 16L ; String "17"; String "18" ]
       
        let elements = [ IntElement ; StringElement ; StringElement ]
        let names = [ "blah";"blah2";"x"]
        let schema = testSchemaCreate elements names
        
        let t1 = Tuple.Create schema d1
        let t2 = Tuple.Create schema d2
        let t3 = Tuple.Create schema d3
        let t4 = Tuple.Create schema d4
        let t5 = Tuple.Create schema d5
        let t6 = Tuple.Create schema d6
        
        let outerSchema = testSchemaCreate [ IntElement; SequenceElement schema  ] ["Foo";"MyTuples"]
        let outerTup = Tuple.Create outerSchema [ Int 1001L; Tuples (seq [ t1;t2;t3;t4;t5;t6]  |> TupleSequence.Create schema) ]
        
        let wSer = new System.IO.MemoryStream ()
        let wReser = new System.IO.MemoryStream ()
        
            
        Aqueduct.Serialization.serializeTuple  formatter wSer outerTup
        wSer.Position <- 0L
        let deser = Aqueduct.Serialization.deserializeTuple outerTup.schema formatter wSer
        Aqueduct.Serialization.serializeTuple formatter wReser deser
        wReser.Position <- 0L
        let reser = Aqueduct.Serialization.deserializeTuple outerTup.schema formatter wReser
        assertEqual outerTup deser
        assertEqual deser reser
        
        let wSch = new System.IO.MemoryStream ()
        let wReSch = new System.IO.MemoryStream ()
        Aqueduct.Serialization.serializeSchema formatter wSch outerSchema     
        wSch.Position <- 0L   
        let deserSch = Aqueduct.Serialization.deserializeSchema formatter wSch
        Aqueduct.Serialization.serializeSchema formatter wReSch deserSch
        wReSch.Position <- 0L
        let reserSch = Aqueduct.Serialization.deserializeSchema formatter wReSch
        
        assertEqual outerSchema deserSch
        assertEqual outerSchema reserSch
        
        assertEqual outerTup.Arity deser.Arity
        assertEqual outerTup.schema deser.schema                
        Seq.iter2 assertEqual (outerTup.GetTuples "MyTuples").tseq  (deser.GetTuples "MyTuples").tseq
        Seq.iter2 assertEqual (TupDropColumns outerTup ["MyTuples"]).values (TupDropColumns deser ["MyTuples"]).values
        ()
        
    [<Fact>]
    let testSer () =
        testSerFormat Serialization.XmlFormat
        testSerFormat Serialization.BinaryFormat

    [<Fact>]
    let testBinSer() =

        let (assertSerDeserInv, finish) =
            let path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), System.IO.Path.Combine("testBinSer", "seq"))
            let filesystemAssert(tseq : TupleSequence, cmpn : BinarySerialization.Compression) =
                BinarySerialization.Serialize.serializeTupleSequenceToDir(path, tseq, cmpn)
                BinarySerialization.Deserialize.deserializeTupleSequenceAtDir(path) |> assertTseqEq tseq
            let intermediateAssert(tseq : TupleSequence) =
                let intermediate =
                    use stream = new System.IO.MemoryStream()
                    BinarySerialization.Serialize.serializeTupleSequenceToStream stream tseq
                    stream.Flush()
                    stream.Close()
                    stream.ToArray()
                use stream = new System.IO.MemoryStream(intermediate, false)
                BinarySerialization.Deserialize.deserializeTupleSequenceFromStream(stream) |> assertTseqEq tseq
                stream.Close()
            let allModesAssert(tseq : TupleSequence) =
                filesystemAssert(tseq, BinarySerialization.NoCompression)
                //filesystemAssert(tseq, BinarySerialization.Compressed)
                intermediateAssert(tseq)
            let finish() =
                System.IO.Directory.Delete(path, true)
                System.IO.File.Delete(path)
            in (allModesAssert, finish)
        let shallowSchema = Schema.Make [("foo", IntElement); ("bar", StringElement)]
        let    tinySchema = Schema.Make [("zab", FloatElement)]
        let    deepSchema = Schema.Make [("foo", IntElement); ("bar", StringElement); ("baz", SequenceElement tinySchema)]
        let  littleSchema = Schema.Make [("zab", SequenceElement tinySchema); ("blub", StringElement)]
        let  deeperSchema = Schema.Make [("foo", UnsignedElement); ("bar", StringElement); ("baz", SequenceElement littleSchema)]
        let mkseqShallow = TupleSequence.Create shallowSchema
        let mkseqDeep    = TupleSequence.Create    deepSchema
        let mkseqTiny    = TupleSequence.Create    tinySchema
        let mkseqDeeper  = TupleSequence.Create  deeperSchema
        let mkseqLittle  = TupleSequence.Create  littleSchema
        let mktupShallow (foo : int64) (bar : string) = Tuple.Create shallowSchema [Datum.Int foo; Datum.String bar]
        let mktupDeep (foo : int64) (bar : string) (baz : Tuple seq) = Tuple.Create deepSchema [Datum.Int foo; Datum.String bar; Datum.Tuples <| mkseqTiny baz]
        let mktupTiny (zab : double) = Tuple.Create tinySchema [Datum.Float zab]
        let mktupDeeper (foo : uint64) (bar : string) (baz : Tuple seq) = Tuple.Create deeperSchema [Datum.Unsigned foo; Datum.String bar; Datum.Tuples <| mkseqLittle baz]
        let mktupLittle (zab : Tuple seq) (blub : string) = Tuple.Create littleSchema [Datum.Tuples <| mkseqTiny zab; Datum.String blub]

        let emptyShallow = mkseqShallow Seq.empty
        let emptyDeep    = mkseqDeep    Seq.empty
        let emptyDeeper  = mkseqDeeper  Seq.empty
        let shortShallow = mkseqShallow [mktupShallow 0L "zero"; mktupShallow 1L "one"; mktupShallow 2L "two"]
        let shortDeep    = mkseqDeep [mktupDeep 12L "twelve" [mktupTiny 0.0]; mktupDeep 20L "twenty" []; mktupDeep 42L "forty-two" [mktupTiny 3.1416]]
        let shortDeeper  = mkseqDeeper [mktupDeeper 1UL "foo = 1" [mktupLittle [mktupTiny 1.0; mktupTiny 1.1; mktupTiny 1.2] "hello"
                                                                   mktupLittle [mktupTiny 2.0; mktupTiny 2.1; mktupTiny 2.2] "world"]
                                        mktupDeeper 2UL "foo = 2" []
                                        mktupDeeper 3UL "foo = 3" [mktupLittle [] ""; mktupLittle [] ""]]
        let  longShallow = mkseqShallow [for i in [1L  .. 1000L ] do yield mktupShallow i " "]
        let  longDeep    = mkseqDeep    [for i in [1L  ..  100L ] do yield mktupDeep i "\t" [for i' in [0 .. 10] do yield mktupTiny (i' |> double)]]
        let  longDeeper  = mkseqDeeper  [for i in [1UL ..   10UL] do
                                             yield mktupDeeper i "\n" [
                                                 for i' in [0 .. 10] do
                                                     yield mktupLittle [
                                                         for i'' in [0 .. 10] do
                                                             yield mktupTiny (i'' |> double)] "\0"]]

        assertSerDeserInv(emptyShallow)
        assertSerDeserInv(emptyDeep)
        assertSerDeserInv(emptyDeeper)
        assertSerDeserInv(shortShallow)
        assertSerDeserInv(shortDeep)
        assertSerDeserInv(shortDeeper)
        assertSerDeserInv(longShallow)
        assertSerDeserInv(longDeep)
        assertSerDeserInv(longDeeper)
        finish()

    [<SetUpDb>]
    [<Fact>]
    let testDb () =                
                        
        let longstr = Array.create 0x2000 (90|>byte) |> System.Text.Encoding.ASCII.GetString 
        let d1 = [ Int 1L ; String "2"; String "3" ]
        let d2 = [ Int 4L ; String "5"; String "6" ]
        let d3 = [ Int 7L ; String "8"; String "9" ]
        let d4 = [ Int 10L ; String "11"; String "12" ]
        let d5 = [ Int 13L ; String "14"; String "15" ]
        let d6 = [ Int 16L ; String "17"; String longstr ]
       
        let elements = [ IntElement ; StringElement ; StringElement ]
        let names = [ "blah";"blah2";"x"]
        let schema = testSchemaCreate elements names
        
        let t1 = Tuple.Create schema d1
        let t2 = Tuple.Create schema d2
        let t3 = Tuple.Create schema d3
        let t4 = Tuple.Create schema d4
        let t5 = Tuple.Create schema d5
        let t6 = Tuple.Create schema d6
        
        let sseq = seq [ t1;t2;t3;t4;t5;t6 ] |> TupleSequence.Create schema
        let slist = sseq.tseq |> List.ofSeq
        let outerSchema = testSchemaCreate [ StringElement; SequenceElement schema ] ["Foo";"MyTuples"]
        let outerTup = Tuple.Create outerSchema [ String "some string that will get used as a key. Make it really long so we need to use the md5 to make it shorter."; 
                                                  Tuples sseq ]
                              
        let table =  MetricsStorageTable.Create ()
        let keygen (tup:Tuple) = [tup.GetDatum "blah"]
        table.AddSeq keygen sseq
        let result = (table.Read ()).tseq |> List.ofSeq
        assertEqual (Seq.length sseq.tseq)  (Seq.length result)        
        assertEqual slist.Head.schema result.Head.schema
        Seq.iter2 assertEqual sseq.tseq result
        
        let foundT3Seq = ["blah" |> t3.GetDatum] |> table.Find 
        assertEqual 1 (Seq.length foundT3Seq.tseq)
        let foundT3 = foundT3Seq.tseq |> Seq.head
        assertEqual t3 foundT3
                
        let table2 =  MetricsStorageTable.Create ()
        let keygen (tup:Tuple) = [tup.GetDatum "Foo"]
        table2.AddSeq keygen (outerTup |> Seq.singleton |> TupleSequence.Create outerTup.schema)
            
        let expected2 = TupleSequence.Create outerTup.schema (Seq.singleton outerTup)
        
        let result2 = table2.Read () 
        assertTseqEq expected2  result2        
        ()       
        
    let testInnerJoin joinSize =                
                
        let ls = schema3
                       ["field2";"field3";"bar"]
                       [FloatElement;IntElement; StringElement] 
                       [Float -2.0; Int -2L; String "def3"] 
                                    
        let rs = schema3
                   ["field1";"foo";"qux"]
                   [IntElement;StringElement;StringElement] 
                   [Int -1L; String "def1"; String "def2"]
                                                
        let js = schema3
                  ["field2";"field3";"bar";"foo";"qux"]
                  [FloatElement; IntElement; StringElement; StringElement; StringElement] 
                  [Float -2.0; Int -2L; String "def3"; String "def1"; String "def2"] 
                            
        assertEqual js ((ls.Concat rs).DropColumns ["field1"])
        
        let t1r = Tuple.Create rs [Int 1L; String "match1"; String "one"]
        let t2r = Tuple.Create rs [Int 2L; String "no-match1"; String "two"]
        let t3r = Tuple.Create rs [Int 3L; String "match2"; String "three"]
        
        let t1l = Tuple.Create ls [Float 4.0; Int 1L; String "match2"]
        let t2l = Tuple.Create ls [Float 5.0; Int 3L; String "match1"]
        let t3l = Tuple.Create ls [Float 6.0; Int 5L; String "no-match2"]
        let t4l = Tuple.Create ls [Float 7.0; Int 7L; String "match2"]
                       
        let e1 = Tuple.Create js [Float 4.0; Int 1L; String "match2"; String "match1"; String "one"]                 
        let e2 = Tuple.Create js [Float 5.0; Int 3L; String "match1"; String "match2"; String "three"] 
        
        
        let left = [t1l;t2l;t3l;t4l]
        let right = [t1r;t2r;t3r]
        
        let exKey = SchemaKey.Create([Ascending,"field3"],false)
        let expected:TupleSequence = [e1;e2] |> Seq.ofList |> TupleSequence.CreateKeyed (js.SetKey exKey)
        let expectedX:TupleSequence = Seq.empty |> TupleSequence.CreateKeyed (js.SetKey exKey)
        
        let lkey = SchemaKey.Create([Ascending,"field3"],false)
        let rkey = SchemaKey.Create([Ascending,"field1"],false)
        let ltap = Tap.Create (fun () -> left |> Seq.ofList |> TupleSequence.CreateKeyed (ls.SetKey lkey)) 
        let rtap = Tap.Create (fun () -> right |> Seq.ofList |> TupleSequence.CreateKeyed (rs.SetKey rkey))
                
        let rpipe = "right" *> IdentityPipe       
        let rpipeX = "right" *> SimpleFilter (fun _ -> false)
        
        
        // note we insert the chop pipe between the join and the sort because otherwise we end up locking sqlite --
        // we're reading from one sequence that is coming from the database while we're writing to another. And unfortunately,
        // Sqlite only doesn't even allow reads when there is a write going on, even to different tables.
        let lpipe = "left" *> IdentityPipe <**> (Join (joinSize,JoinInner) ["field3"] ["field1"],rpipe) <*> IdentityPipe       
        let lpipe2 = "left" *> IdentityPipe <**> (Join (joinSize,JoinInner) ["field3"] ["field1"],rpipeX) <*> IdentityPipe
        
        let taps = ["left",ltap;"right",rtap]
        let sink = new CollectorSink()        
        let sinkX = new CollectorSink()
        let tapped = RunPipes sink lpipe taps        
        let tappedX = RunPipes sinkX lpipe2 taps
                
        let result = sink.GetSeq
        let resultX = sinkX.GetSeq
                
        assertTseqEq expected result
        assertTseqEq expectedX resultX
        ()
         
          
    
    let testLeftOuterJoin joinSize =                
                
        let rs = schema3
                    ["field1";"foo";"qux";"foo2"]
                    [IntElement;StringElement;StringElement; StringElement] 
                    [Int System.Int64.MinValue;String "*default*";String "*default*"; String"*default*"] 
                                                
        let ls = testSchemaCreate [FloatElement;IntElement; StringElement; StringElement] ["field2";"field3";"bar";"bar2"]
        let js = ((ls.Concat rs).DropColumns ["foo";"foo2"])        
                                
        let t1l = Tuple.Create ls [Float 5.0; Int 5L; String "aa"; String "AA"]                        
        let t2l = Tuple.Create ls [Float 4.0; Int 4L; String "bb"; String "BB"]        
        let t3l = Tuple.Create ls [Float 7.0; Int 7L; String "bb"; String "BB"]
        let t4l = Tuple.Create ls [Float 6.0; Int 6L; String "xx"; String "XX"]
        
        let t1r = Tuple.Create rs [Int 1L; String "aa"; String "one"; String "AA"]
        let t2r = Tuple.Create rs [Int 2L; String "ab"; String "two"; String "AB"]
        let t3r = Tuple.Create rs [Int 3L; String "bb"; String "three"; String "BB"]
        
        let e1 = Tuple.Create js [Float 5.0; Int 5L; String "aa"; String "AA"; Int 1L; String "one"]        
        let e2 = Tuple.Create js [Float 4.0; Int 4L; String "bb"; String "BB"; Int 3L; String "three"] 
        let e3 = Tuple.Create js [Float 7.0; Int 7L; String "bb"; String "BB"; Int 3L; String "three"] 
        let e4 = Tuple.Create js [Float 6.0; Int 6L; String "xx"; String "XX"; Int System.Int64.MinValue; String "*default*"] 
                                             
        let keyL = SchemaKey.Create ([Ascending,"bar";Ascending,"bar2"],true)
        let keyR = SchemaKey.Create ([Ascending,"foo";Ascending,"foo2"],false)
        let keyEx = SchemaKey.Create ([Ascending,"bar";Ascending,"bar2"],false)
        let createLeft = TupleSequence.CreateKeyed (ls.SetKey keyL)
        let createRight = TupleSequence.CreateKeyed (rs.SetKey keyR)
        let createEx = TupleSequence.CreateKeyed (js.SetKey keyEx)
        let left = seq [t1l;t2l;t3l;t4l] |> createLeft
        let right = seq [t1r;t2r;t3r] |> createRight
        let expected = seq [e1;e2;e3;e4]  |> createEx
        
        let left2 = seq [t4l] |> createLeft
        let right2 = right
        let ex2 = seq [e4] |> createEx
        
        let left3 = Seq.empty |> createLeft
        let right3 = right
        let ex3 = Seq.empty |> createEx
        
        let left4 = left3
        let right4 = Seq.empty |> createRight
        let ex4 = ex3
                                                               
        let runJoinPipe ex  l r =            
            let ltap = Tap.Create (fun () -> l ) 
            let rtap = Tap.Create (fun () -> r )
            let rpipe = "right" *> IdentityPipe
            let rpipeC = "right" *> IdentityPipe
            // note we insert the chop pipe between the join and the sort because otherwise we end up locking sqlite --
            // we're reading from one sequence that is coming from the database while we're writing to another. And unfortunately,
            // Sqlite only doesn't even allow reads when there is a write going on, even to different tables.
            let lpipe = "left" *> IdentityPipe <**> (Join (joinSize,JoinLeftOuter) ["bar";"bar2"] ["foo";"foo2"],rpipe) <*> IdentityPipe            
            let taps = ["left",ltap;"right",rtap]
            
            let sink = new CollectorSink()          
            let tapped = RunPipes sink lpipe taps
                           
            let result = sink.GetSeq             
            assertTseqEq ex result
            
        runJoinPipe expected left right        
        runJoinPipe ex2 left2 right2
        runJoinPipe ex3 left3 right3
        runJoinPipe ex4 left4 right4           
        ()
        
    let testFullOuterJoin joinSize =                
        
        let ls =  schema3
                    ["field2";"field3";"bar";"bar2"]        
                    [FloatElement;IntElement; StringElement; StringElement] 
                    [Float -1.0;Int -10L;String "default1"; String "default2"] 
        let rs = schema3
                      ["field1";"foo";"qux";"foo2"]
                      [IntElement;StringElement;StringElement; StringElement] 
                      [Int System.Int64.MinValue;String "*default*";String "*default*"; String"*default*"] 
                                                
        
        let js = ((ls.Concat rs).DropColumns ["foo";"foo2"])        
                                
        let t1l = Tuple.Create ls [Float 5.0; Int 5L; String "aa"; String "AA"]                        
        let t2l = Tuple.Create ls [Float 4.0; Int 4L; String "bb"; String "BB"]
        let t3l = Tuple.Create ls [Float 6.0; Int 6L; String "xx"; String "XX"]
        
        let t1r = Tuple.Create rs [Int 1L; String "aa"; String "one"; String "AA"]
        let t2r = Tuple.Create rs [Int 2L; String "aa"; String "two"; String "BB"]
        let t3r = Tuple.Create rs [Int 3L; String "bb"; String "three"; String "BB"]
        
        let e1 = Tuple.Create js [Float 5.0; Int 5L; String "aa"; String "AA"; Int 1L; String "one"]        
        let e2 = Tuple.Create js [Float -1.0; Int -10L; String "aa"; String "BB"; Int 2L; String "two"] 
        let e3 = Tuple.Create js [Float 4.0; Int 4L; String "bb"; String "BB"; Int 3L; String "three"]                 
        let e4 = Tuple.Create js [Float 6.0; Int 6L; String "xx"; String "XX"; Int System.Int64.MinValue; String "*default*"] 

        let keyL = SchemaKey.Create ([Ascending,"bar";Ascending,"bar2"],true)
        let keyR = SchemaKey.Create ([Ascending,"foo";Ascending,"foo2"],true)
        let keyEx = SchemaKey.Create ([Ascending,"bar";Ascending,"bar2"],true)
                                                     
        let createLeft foo = TupleSequence.CreateKeyed (ls.SetKey keyL) foo
        let createRight foo = TupleSequence.CreateKeyed (rs.SetKey keyR)foo
        let createEx foo = 
            let key = if joinSize = JoinOrdered then keyEx else  SchemaKey.Empty.SetUnique true
            TupleSequence.CreateKeyed (js.SetKey key) foo
        let left = seq [t1l;t2l;t3l] |> createLeft
        let right = seq [t1r;t2r;t3r] |> createRight
        let expected = seq [e1;e2;e3;e4]  |> createEx
        
        let left2 = seq [t3l] |> createLeft
        let right2 = seq[t2r] |> createRight
        let ex2 = seq [e2;e4] |> createEx
        
        let left3 = Seq.empty |> createLeft
        let right3 = seq [t2r] |> createRight
        let ex3 = seq [e2] |> createEx
        
        let left4 = seq[t3l] |> createLeft
        let right4 = Seq.empty |> createRight
        let ex4 = seq [e4] |> createEx
        
        let left5 = Seq.empty |> createLeft
        let right5 = Seq.empty |> createRight
        let ex5 = Seq.empty |> createEx
        
                                                               
        let runJoinPipe ex  l r =            
            let ltap = Tap.Create (fun () -> l )
            let rtap = Tap.Create (fun () -> r ) 
            let rpipe = "right" *> IdentityPipe
            let rpipeC = "right" *> IdentityPipe
            // note we insert the chop pipe between the join and the sort because otherwise we end up locking sqlite --
            // we're reading from one sequence that is coming from the database while we're writing to another. And unfortunately,
            // Sqlite only doesn't even allow reads when there is a write going on, even to different tables.
            let lpipe = "left" *> IdentityPipe <**> (Join (joinSize,JoinFullOuter) ["bar";"bar2"] ["foo";"foo2"],rpipe) <*> IdentityPipe             
            let taps = ["left",ltap;"right",rtap]
            
            let sink = new CollectorSink()            
            let tapped = RunPipes sink lpipe taps                           
            let result = sink.GetSeq            
 
            assertTseqEq ex result
            
        runJoinPipe expected left right        
        runJoinPipe ex2 left2 right2
        runJoinPipe ex3 left3 right3
        runJoinPipe ex4 left4 right4           
        runJoinPipe ex5 left5 right5
        ()
        
    [<SetUpDb>]
    [<Fact>]
    let testUniqueId () =                
                
        let s = testSchemaCreate [IntElement;StringElement;StringElement] ["field1";"foo";"qux"]
        
        
        let t1 = Tuple.Create s [Int 1L; String "match1"; String "one"]
        let t2 = Tuple.Create s [Int 0L; String "no-match1"; String "two"]
        let t3 = Tuple.Create s [Int 3L; String "match2"; String "three"]
        let t4 = Tuple.Create s [Int 0L; String "match2"; String "three"]
        let t5 = Tuple.Create s [Int 0L; String "match2"; String "three"]
        let t6 = Tuple.Create s [Int 2L; String "match2"; String "three"]
                                       
        let e1 = t1
        let e2 = Tuple.Create s [Int 4L; String "no-match1"; String "two"]
        let e3 = t3
        let e4 = Tuple.Create s [Int 5L; String "match2"; String "three"]
        let e5 = Tuple.Create s [Int 6L; String "match2"; String "three"]
        let e6 = t6
                
        let input = seq [t1;t2;t3;t4;t5;t6] |> TupleSequence.Create s       
        let key = SchemaKey.Create([Ascending,"field1"],true)         
        let expected = seq [e1;e2;e3;e4;e5;e6] |> TupleSequence.CreateKeyed (s.SetKey key)
        
        let tap = Tap.Create (fun () -> input)
        
                
        let pipe= "tap" *> IdentityPipe <*> AssignUniqueIds "field1"       
                                        
        let result = (TestRunPipe ("tap",tap) pipe)        
        
        assertTseqEq expected result
        () 
        
    [<Fact>]
    let testFlatten () =
        
        let innerS = testSchemaCreate [IntElement;StringElement] ["field1";"foo"]
        let outerS = testSchemaCreate [FloatElement;FloatElement; StringElement;SequenceElement innerS] ["field2";"field3";"bar";"nested"]
        let flatS = testSchemaCreate [FloatElement; FloatElement; StringElement; IntElement;StringElement] ["field2";"field3";"bar";"field1";"foo"]
        
        let in1 = [Int 1L;String "a"] |> Tuple.Create innerS
        let in2 = [Int 2L;String "b"] |> Tuple.Create innerS
        let in3 = [Int 3L;String "c"] |> Tuple.Create innerS
        let ts1 = seq [in1;in2;in3] |> TupleSequence.Create innerS
        let ts2 = seq [in3] |> TupleSequence.Create innerS
        let out1 = [Float 1.0;Float 10.0; String "A";Tuples ts1] |> Tuple.Create outerS
        let out2 = [Float 2.0;Float 20.0; String "B";Tuples ts2] |> Tuple.Create outerS        
        let ex1 = [Float 1.0;Float 10.0; String "A";Int 1L;String "a"] |> Tuple.Create flatS
        let ex2 = [Float 1.0;Float 10.0; String "A";Int 2L;String "b"] |> Tuple.Create flatS
        let ex3 = [Float 1.0;Float 10.0; String "A";Int 3L;String "c"] |> Tuple.Create flatS
        let ex4 = [Float 2.0;Float 20.0; String "B";Int 3L;String "c"] |> Tuple.Create flatS        
        let ex = seq [ex1;ex2;ex3;ex4]  |> TupleSequence.Create flatS
        
        let tap = Tap.Create (fun () -> seq [out1;out2] |> TupleSequence.Create outerS) 
        let pipe = "tap" *> Flatten "nested"
        let sink = new CollectorSink ()
        let tapped = RunPipes sink pipe ["tap",tap]
        let res = sink.GetSeq
        assertTseqEq ex res
                
    [<Fact>]
    let testReorder () =
        
        let schema = testSchemaCreate [StringElement;IntElement;IntElement] ["field1";"field2";"field3"]
        let reorderedSchema = testSchemaCreate [IntElement;IntElement;StringElement] ["field2";"field3";"field1"]
   
        let s1 = [String "a"; Int 2L; Int 3L] |> Tuple.Create schema
        let s2 = [String "b"; Int 1L; Int 2L] |> Tuple.Create schema
        let ts = seq [s1; s2] |> TupleSequence.Create schema
        
        let rs1 = [Int 2L; Int 3L; String "a"] |> Tuple.Create reorderedSchema
        let rs2 = [Int 1L; Int 2L; String "b"] |> Tuple.Create reorderedSchema
        let rts = seq [rs1; rs2] |> TupleSequence.Create reorderedSchema
     
        let tap = Tap.Create (fun () -> ts) 
        let pipe = "tap" *> ReorderColumnsBy (fun sch name -> if name = "field1" then System.Int32.MaxValue else sch.NameIndex name)
        let sink = new CollectorSink ()
        let tapped = RunPipes sink pipe ["tap",tap]
        let res = sink.GetSeq
        assertTseqEq rts res 
        
    [<Fact>]
    let testAddOuterColumn () =
        
        let innerS = testSchemaCreate [IntElement;StringElement] ["field1";"foo"]
        let innerS2 = testSchemaCreate [IntElement] ["field1"]
        let outerS = testSchemaCreate [FloatElement;FloatElement; StringElement;SequenceElement innerS] 
                                   ["field2";"field3";"bar";"nested"]
        let exS = testSchemaCreate [StringElement;FloatElement; FloatElement; StringElement; SequenceElement innerS] 
                                ["foo";"field2";"field3";"bar";"nested"]
        
        let in1 = [Int 1L;String "a"] |> Tuple.Create innerS
        let in2 = [Int 2L;String "b"] |> Tuple.Create innerS
        let in3 = [Int 3L;String "c"] |> Tuple.Create innerS
        let inE1 = [Int 1L] |> Tuple.Create innerS2
        let inE2 = [Int 2L] |> Tuple.Create innerS2
        let inE3 = [Int 3L] |> Tuple.Create innerS2

        let ts1 = seq [in1;in2;in3] |> TupleSequence.Create innerS
        let ts2 = seq [in3] |> TupleSequence.Create innerS
        
        let tsE1 = seq [inE1;inE2;inE3] |> TupleSequence.Create innerS2
        let tsE2 = seq [inE3] |> TupleSequence.Create innerS2
        
        let out1 = [Float 1.0;Float 10.0; String "A";Tuples ts1] |> Tuple.Create outerS
        let out2 = [Float 2.0;Float 20.0; String "B";Tuples ts2] |> Tuple.Create outerS        
        let ex1 = [String "a";Float 1.0;Float 10.0; String "A";Tuples ts1] |> Tuple.Create exS
        let ex2 = [String "c";Float 2.0;Float 20.0; String "B";Tuples ts2] |> Tuple.Create exS
        let ex = [ex1;ex2]
        
        let tap = Tap.Create (fun () -> seq [out1;out2] |> TupleSequence.Create outerS)
        let pipe = "tap" *> AddOuterColumn (Column.Make("foo",StringElement)) "nested"
                            (fun (tuples:TupleSequence) ->
                                let tup = Seq.head tuples.tseq
                                tup.GetDatum "foo")
        let sink = new CollectorSink ()
        let tapped = RunPipes sink pipe ["tap",tap]
        let res = sink.GetSeq.tseq |> List.ofSeq
        assertEqual (List.length ex) (List.length res)
        List.iter2 (fun e r -> assertEqual e.schema r.schema) ex res
        List.iter2 (fun e r -> List.iter2 (fun ed rd -> assertEqual ed rd) e.values r.values) ex res                        
        
    [<Fact>]
    let testAddInnerColumn () =
        
        let innerS = testSchemaCreate [IntElement;StringElement] ["field1";"foo"]        
        let outerS = testSchemaCreate [FloatElement;FloatElement; StringElement;SequenceElement innerS] 
                                   ["field2";"field3";"bar";"nested"]
        let exInnerS = innerS.Add (sc2("new",IntElement))
        
        let exS = testSchemaCreate [FloatElement; FloatElement; StringElement; SequenceElement exInnerS] 
                                ["field2";"field3";"bar";"nested"]
        
        let in1 = [Int 1L;String "a"] |> Tuple.Create innerS
        let in2 = [Int 2L;String "b"] |> Tuple.Create innerS
        let in3 = [Int 3L;String "c"] |> Tuple.Create innerS        
        
        let ts1 = seq [in1;in2;in3] |> TupleSequence.Create innerS
        let ts2 = seq [in3] |> TupleSequence.Create innerS
        
        let inE1 = TupAddColumn in1 "new" (Int 11L)
        let inE2 = TupAddColumn in2 "new" (Int 12L)
        let inE3 = TupAddColumn in3 "new" (Int 13L)
        let inE4 = TupAddColumn in3 "new" (Int 23L)
                
        let tsE1 = seq [inE1;inE2;inE3] |> TupleSequence.Create exInnerS
        let tsE2 = seq [inE4] |> TupleSequence.Create exInnerS
        
        let out1 = [Float 1.0;Float 10.0; String "A";Tuples ts1] |> Tuple.Create outerS
        let out2 = [Float 2.0;Float 20.0; String "B";Tuples ts2] |> Tuple.Create outerS        
        let ex1 = [Float 1.0;Float 10.0; String "A";Tuples tsE1] |> Tuple.Create exS
        let ex2 = [Float 2.0;Float 20.0; String "B";Tuples tsE2] |> Tuple.Create exS
        let ex = seq [ex1;ex2] |> TupleSequence.Create exS
        
        let inSeq = seq [out1;out2] |> TupleSequence.Create outerS
        let addSchema = Schema.Empty.Add (sc2("new",IntElement))
        let addInnerFun (outer:Tuple) (inner:Tuple) =  
            let outerVal = outer.GetFloat "field3" |> int64
            let innerVal = inner.GetInt "field1" 
            let newVal = outerVal + innerVal |> Datum.Int
            Tuple.Create addSchema [newVal]
                        
        let pipe = AddInnerColumns "nested" addSchema addInnerFun 
                            
        let res = RunPipeFromSeq inSeq pipe
        assertTseqEq ex res
        ()       
        
    let assertOrdering expOrd =
        MapPipe.Create(sprintf "assertOrd(%A)" expOrd,(fun x->x),(fun ord->assertEqual expOrd ord; ord),fun sch->sch)

    [<Fact>]
    let testMemoize () =
        let sch = testSchemaCreate [IntElement;StringElement;FloatElement] ["field1";"field2";"field3"]                        
        let in1 = [Int 1L;String "a";Float 0.1] |> Tuple.Create sch
        let in2 = [Int 1L;String "a";Float 0.1] |> Tuple.Create sch
        let in3 = [Int 2L;String "b";Float 0.2] |> Tuple.Create sch
        let in4 = [Int 2L;String "b";Float 0.2] |> Tuple.Create sch
        let in5 = [Int 3L;String "c";Float 0.3] |> Tuple.Create sch
        let in6 = [Int 3L;String "c";Float 0.3] |> Tuple.Create sch
        let inseq = seq [in1;in2;in3;in4;in5;in6]
        let intseq = TupleSequence.Create sch inseq
        
        let pipe = IdentityPipe 
        let res = RunPipeFromSeq intseq pipe        
        assertTseqEq intseq res
        let enum = res.tseq.GetEnumerator ()
        while enum.MoveNext () do
            let left = enum.Current
            enum.MoveNext () |> ignore
            let right = enum.Current
            let check l r =
                assertReferenceEqual l r
            List.iter2 check left.values right.values
        
        
    [<SetUpDb>]
    [<Fact>]
    let testOrdering1 () = 
        let fookey = SchemaKey.Create ([Ascending,"foo"])
        let foobarkey = SchemaKey.Create ([Ascending,"foo";Ascending,"bar"])
        let fookeyEx = fookey.SetUnique true
        let foobarkeyEx = foobarkey.SetUnique true
        let barkey = SchemaKey.Create ([Ascending,"bar"])
        let bazkey = SchemaKey.Create ([Ascending,"baz"])
        let pipe1 =
            "tap" 
            *> AssumeOrdering fookey
            <*> StripQuotes 
            <*> PartialSchemaApply (Schema.Make ([("bar", IntElement)]))
            <*> AddColumnsRegex ("baz",["as"],"^a+")
            <*> DropColumns ["baz"] 
            <*> Limit (0L,100L)
            <*> PrintStatus "print" 
            <*> assertOrdering fookey
            <*> UpdateIntColumn "bar" (fun _ x -> x + 1L)
            <*> assertOrdering fookey
                                               
        let pipe2 size =
            "tap2"
            *> AssumeOrdering barkey
           <*> PartialSchemaApply (Schema.Make [("bar", IntElement)])
           //<*> assertOrdering []                  
           <*> AssumeOrdering barkey
           <*> Group size ["foo"] "grouped"
           <*> assertOrdering fookeyEx
           <*> Flatten "grouped"
           <*> assertOrdering foobarkey
           <*> Group size ["foo"] "grouped"
           <*> assertOrdering fookeyEx
           
        let pipe2o =
            "tap2"
            *> AssumeOrdering barkey
           <*> PartialSchemaApply (Schema.Make [("bar", IntElement)])
           //<*> assertOrdering []                  
           <*> AssumeOrdering foobarkey
           <*> Group JoinOrdered ["foo"] "grouped"
           <*> assertOrdering fookeyEx
           <*> Flatten "grouped"
           <*> assertOrdering foobarkey
           <*> Group JoinOrdered ["foo"] "grouped"
           <*> assertOrdering fookeyEx
                           
                   
        let pipe3 =
            "tap3" 
            *> AssumeOrdering bazkey
            <*> DropColumns ["baz"] 
            <*> assertOrdering SchemaKey.Empty

        let pipe5 =
            "tap5" *> PartialSchemaApply (Schema.Make [("bar", IntElement)]) <*>  AssumeOrdering barkey
            <*> UpdateIntColumn "bar" (fun _ x -> x + 1L) <*> assertOrdering SchemaKey.Empty
                 
        
        let pipe6 =
            "tap6" *> 
            AssumeOrdering fookey <*>  ToLower ["bar"] <*> assertOrdering fookey
            <*> ToLower ["foo"] <*> assertOrdering SchemaKey.Empty
                 
        
        let pipe7 =
            "tap7" *>
            AssumeOrdering fookey <*>  assertOrdering fookey
            <*>  KeepColumns ["bar"] <*> assertOrdering SchemaKey.Empty
                                
        let s1 = testSchemaCreate [ StringElement; StringElement ; StringElement ] ["foo";"bar";"baz"] 
        let t1 = Tuple.Create s1 (["a";"1";"x"] |> List.map String)
        
        let s2 = testSchemaCreate [ StringElement; StringElement ; StringElement ] ["foo";"qux";"blah"] 
        let t2 = Tuple.Create s1 (["a";"2";"y"] |> List.map String)
        
        
        let tap = {new ITap with member x.Tap () = (seq [t1] |> TupleSequence.Create s1)}
        TestRunPipe ("tap",tap) pipe1 |> ignore
        TestRunPipe ("tap2",tap) (pipe2 JoinSmall) |> ignore
        TestRunPipe ("tap2",tap) (pipe2 JoinLarge) |> ignore
        TestRunPipe ("tap2",tap) pipe2o |> ignore
        TestRunPipe ("tap3",tap) pipe3 |> ignore
        TestRunPipe ("tap5",tap) pipe5 |> ignore
        TestRunPipe ("tap6",tap) pipe6 |> ignore
        TestRunPipe ("tap7",tap) pipe7 |> ignore
        () 
        
    let private clickSchema = testSchemaCreate [ IntElement; IntElement; IntElement; StringElement; IntElement; ]
                                            [ "local_day_week"; "local_hour_day"; "expected_rule"; "zip"; "city_conf"; ]
    
    [<SetUpDb>]                          
    [<Fact>] 
    let testJoins () =
        for joinSize in [JoinSmall;JoinLarge;JoinOrdered] do
            testLeftOuterJoin joinSize        
            testInnerJoin joinSize
            if joinSize <> JoinLarge then 
                testFullOuterJoin joinSize
    
    [<Fact>]    
    let testOrderedJoinOuter () =                
                
        let rs = (testSchemaCreate [IntElement;StringElement;StringElement] ["field1";"foo";"qux"])
        let ls = (testSchemaCreate [FloatElement;IntElement; StringElement] ["field2";"field3";"bar"])
        let js = (testSchemaCreate [FloatElement; IntElement; StringElement; StringElement; StringElement] 
                                   ["field2";"field3";"bar";"foo";"qux"])
        assertEqual js ((ls.Concat rs).DropColumns ["field1"])
        
        let t1r = Tuple.Create rs [Int 1L; String "match1"; String "one"]
        let t2r = Tuple.Create rs [Int 2L; String "no-match1"; String "two"]
        let t3r = Tuple.Create rs [Int 3L; String "match2"; String "three"]
        
        let t1l = Tuple.Create ls [Float 4.0; Int 1L; String "match2"]
        let t2l = Tuple.Create ls [Float 5.0; Int 3L; String "match1"]
        let t3l = Tuple.Create ls [Float 6.0; Int 5L; String "no-match2"]
        let t4l = Tuple.Create ls [Float 7.0; Int 7L; String "match2"]
                       
        let e1 = Tuple.Create js [Float 4.0; Int 1L; String "match2"; String "match1"; String "one"]
        let e2 = Tuple.Create js [Float 5.0; Int 3L; String "match1"; String "match2"; String "three"]
        let e3 = Tuple.Create js [Float 6.0; Int 5L; String "no-match2"; String ""; String ""] 
        let e4 = Tuple.Create js [Float 7.0; Int 7L; String "match2"; String ""; String ""]
        let eX1 = Tuple.Create js [Float 4.0; Int 1L; String "match2"; String ""; String ""]
        let eX2 = Tuple.Create js [Float 5.0; Int 3L; String "match1"; String ""; String ""]
        let eX3 = e3
        let eX4 = e4
        
        let left = [t1l;t2l;t3l;t4l]
        let right = [t1r;t2r;t3r]
        
        let exKey = SchemaKey.Create([Ascending,"field3"],false)
        let expected:TupleSequence = [e1;e2;e3;e4] |> Seq.ofList |> TupleSequence.CreateKeyed (js.SetKey exKey)
        let expectedX:TupleSequence = [eX1;eX2;eX3;eX4] |> Seq.ofList |> TupleSequence.CreateKeyed (js.SetKey exKey)
                
        let lkey = SchemaKey.Create([Ascending,"field3"],true)
        let rkey= SchemaKey.Create([Ascending,"field1"],false)
        let ltap = Tap.Create (fun () -> left |> Seq.ofList |> TupleSequence.CreateKeyed (ls.SetKey lkey)) 
        let rtap = Tap.Create (fun () -> right |> Seq.ofList |> TupleSequence.CreateKeyed (rs.SetKey rkey)) 
                
        let rpipe = "right" *> IdentityPipe
        let rpipeX = "right" *> SimpleFilter (fun _ -> false)
        
        // note we insert the chop pipe between the join and the sort because otherwise we end up locking sqlite --
        // we're reading from one sequence that is coming from the database while we're writing to another. And unfortunately,
        // Sqlite only doesn't even allow reads when there is a write going on, even to different tables.
        let lpipe = "left" *> IdentityPipe <**> (Join (JoinOrdered,JoinLeftOuter) ["field3"] ["field1"],rpipe) <*> IdentityPipe        
        let lpipeX = "left" *> IdentityPipe <**> (Join (JoinOrdered,JoinLeftOuter) ["field3"] ["field1"],rpipeX) <*> IdentityPipe 
                
        let taps = ["left",ltap;"right",rtap]
        let sink = new CollectorSink()        
        let sinkX = new CollectorSink()
        let tapped = RunPipes sink lpipe taps
        let tappedX = RunPipes sinkX lpipeX taps
                
        let result = sink.GetSeq
        let resultX = sinkX.GetSeq
                
        assertTseqEq expected result
        assertTseqEq expectedX resultX
        () 
    [<Fact>]    
    let testOrderedJoinMerge () =                
                        
        let sch = testSchemaCreate [FloatElement;IntElement; StringElement] ["field2";"field3";"bar"]
                                
        let t1 = Tuple.Create sch [Float 1.0; Int 1L; String "one"]
        let t2 = Tuple.Create sch [Float 2.0; Int 2L; String "two"]
        let t3 = Tuple.Create sch [Float 3.0; Int 3L; String "three"]
        let t4 = Tuple.Create sch [Float 4.0; Int 4L; String "four"]
        let t5 = Tuple.Create sch [Float 5.0; Int 5L; String "five"]
        let t6 = Tuple.Create sch [Float 6.0; Int 6L; String "six"]
        let t7 = Tuple.Create sch [Float 7.0; Int 7L; String "seven"]
        let t8 = Tuple.Create sch [Float 8.0; Int 8L; String "eight"]
                                                                                       
        let key = SchemaKey.Create([Ascending,"field2";Ascending,"field3"],true)
        let exKey = SchemaKey.Create([Ascending, "field2"],false)
        
        let runMerge llist rlist elist =
            let lseq = TupleSequence.CreateKeyed (sch.SetKey key)llist 
            let rseq = TupleSequence.CreateKeyed (sch.SetKey key) rlist
            let eseq = TupleSequence.CreateKeyed  (sch.SetKey exKey) elist
            let ltap = Tap.Create (fun () -> lseq ) 
            let rtap = Tap.Create (fun () -> rseq) 
            let rpipe = "right" *> IdentityPipe                        
                        
            // note we insert the chop pipe between the join and the sort because otherwise we end up locking sqlite --
            // we're reading from one sequence that is coming from the database while we're writing to another. And unfortunately,
            // Sqlite only doesn't even allow reads when there is a write going on, even to different tables.
            let lpipe = "left" *> IdentityPipe <**> (JoinCollateOrdered  ["field2"],rpipe)  <*> IdentityPipe                                    
            
            let taps = ["left",ltap;"right",rtap]
            let sink = new CollectorSink()
            
            let tapped = RunPipes sink lpipe taps                                                                            
            let result = sink.GetSeq
                                
            assertTseqEq eseq result
            
            assertTseqEq eseq (Common.JoinCollateOrderedSequences ["field2"] lseq rseq)
            
        let ex = [t1;t2;t3;t4;t5;t6;t7;t8]
        for ii in 1..8 do
            let l,r = List.zip [1..8] ex |> List.partition (fun (i,tup) -> i % ii = 0) 
            runMerge (l |> List.map snd) (r |> List.map snd) ex
        runMerge [] ex ex
        runMerge  ex [] ex                                            
        () 
            
    let testOrderedColumnMergeSize joinSize =                
                        
        let sch = testSchemaCreate [FloatElement;IntElement; StringElement] ["field2";"field3";"bar"]
                                
        let t1 = Tuple.Create sch [Float 1.0; Int 1L; String "one"]
        let t2 = Tuple.Create sch [Float 2.0; Int 2L; String "two"]
        let t3 = Tuple.Create sch [Float 3.0; Int 3L; String "three"]
        let t4 = Tuple.Create sch [Float 4.0; Int 4L; String "four"]
        let t5 = Tuple.Create sch [Float 5.0; Int 5L; String "five"]
        let t6 = Tuple.Create sch [Float 6.0; Int 6L; String "six"]
        let t7 = Tuple.Create sch [Float 7.0; Int 7L; String "seven"]
        let t8 = Tuple.Create sch [Float 8.0; Int 8L; String "eight"]
                                                                                       
        let key = SchemaKey.Create([Ascending,"field2";Ascending,"field3"],true)
        
        let runMerge llist rlist elist =
            let lseqInner = TupleSequence.CreateKeyed (sch.SetKey key) llist 
            let rseqInner = TupleSequence.CreateKeyed (sch.SetKey key) rlist
            let schInner = Schema.Make ["l",SequenceElement lseqInner.Schema;"r",SequenceElement rseqInner.Schema]
            let t1 = Tuple.Create schInner [Tuples lseqInner;Tuples rseqInner]            
            let t2 = t1
            let tseq = seq [t1;t2]
            let resultKey = 
                // when we do a small join, we don't guarantee order is preserved
                if joinSize = JoinSmall then SchemaKey.Empty.SetUnique true
                else  SchemaKey.Create([Ascending,"field2"],false)
                
            let eseqInner = TupleSequence.CreateKeyed (sch.SetKey resultKey) elist 
            let et1 = Tuple.Create (Schema.Make ["ex",SequenceElement eseqInner.Schema
                                                 "l",SequenceElement lseqInner.Schema
                                                 "r",SequenceElement rseqInner.Schema]) 
                                   [Tuples eseqInner;Tuples lseqInner;Tuples rseqInner]
            let et2 = et1
            let eseq = seq [et1;et2] 
            let esequence = TupleSequence.Create (Seq.head eseq).schema eseq
            let tsequence = TupleSequence.Create (Seq.head tseq).schema tseq
            
            
            let tap = Tap.Create(fun () -> tsequence ) 
            
                                                
            let lpipe = "tap" *> IdentityPipe <*> SequenceFieldMerge joinSize ("l","r","ex") ["field2"]            
                        
            
            let taps = ["tap",tap]
            let sink = new CollectorSink()
            let sinkC = new CollectorSink()
            
            let tapped = RunPipes sink lpipe taps
                                                                            
            let result = sink.GetSeq
                                
            assertTseqEq esequence result
            
        let ex = [t1;t2;t3;t4;t5;t6;t7;t8]
        for ii in 1..8 do
            let l,r = List.zip [1..8] ex |> List.partition (fun (i,tup) -> i % ii = 0) 
            runMerge (l |> List.map snd) (r |> List.map snd) ex
        runMerge [] ex ex
        runMerge  ex [] ex                                            
        () 
        
    [<Fact>]    
    let testOrderedColumnMergeSmall () =                
        testOrderedColumnMergeSize JoinSmall
    
    [<Fact>]    
    let testOrderedColumnMergeOrdered () =                
        testOrderedColumnMergeSize JoinOrdered
        
        
    [<Fact>]
    let testExplodeInnerPipe () =
        let nameMapSch = Schema.Make [("name_idR",IntElement);("name",StringElement)]
        let nameMapSeq = 
            seq [
                Tuple.Create nameMapSch [1L |> Datum.Int;"fieldCol1" |> Datum.String]
                Tuple.Create nameMapSch [2L |> Datum.Int;"fieldCol2" |> Datum.String]
            ] |> TupleSequence.Create nameMapSch
        
        let leftInnerSch = Schema.Create [Column.Create("value",FloatElement,Datum.Float -1.0);
                                          Column.Make("name_idL",IntElement)]
        let leftSch = Schema.Make ["id",IntElement; "values",SequenceElement leftInnerSch]
        
        let inner1 = 
            seq [
                [1.0 |> Datum.Float; 1L |> Datum.Int] |> Tuple.Create leftInnerSch
                [2.0 |> Datum.Float; 2L |> Datum.Int] |> Tuple.Create leftInnerSch
            ] |> TupleSequence.Create leftInnerSch
        let inner2 =
            seq [
                [3.0 |> Datum.Float; 2L |> Datum.Int] |> Tuple.Create leftInnerSch
                [4.0 |> Datum.Float; 1L |> Datum.Int] |> Tuple.Create leftInnerSch
            ] |> TupleSequence.Create leftInnerSch
        let inner3 =
            seq [
                [5.0 |> Datum.Float; 2L |> Datum.Int] |> Tuple.Create leftInnerSch
            ] |> TupleSequence.Create leftInnerSch
        let inner4 = seq [] |> TupleSequence.Create leftInnerSch
            
        let leftSeq =
            seq [
                [1001L |> Datum.Int; inner1 |> Datum.Tuples] |> Tuple.Create leftSch 
                [1002L |> Datum.Int; inner2 |> Datum.Tuples] |> Tuple.Create leftSch 
                [1003L |> Datum.Int; inner3 |> Datum.Tuples] |> Tuple.Create leftSch 
                [1004L |> Datum.Int; inner4 |> Datum.Tuples] |> Tuple.Create leftSch 
            ] |> TupleSequence.Create leftSch
            
        let expectedSch = 
            Schema.Create[Column.Make("id",IntElement)
                          Column.Create("fieldCol1",FloatElement,-1.0 |> Datum.Float)
                          Column.Create("fieldCol2",FloatElement,-1.0 |> Datum.Float)]
            
        let expectedSeq = 
            seq [
                [1001L |> Datum.Int; 1.0 |> Datum.Float; 2.0 |> Datum.Float] |> Tuple.Create expectedSch
                [1002L |> Datum.Int; 4.0 |> Datum.Float; 3.0 |> Datum.Float] |> Tuple.Create expectedSch
                [1003L |> Datum.Int; -1.0 |> Datum.Float; 5.0 |> Datum.Float] |> Tuple.Create expectedSch
                [1004L |> Datum.Int; -1.0 |> Datum.Float; -1.0 |> Datum.Float] |> Tuple.Create expectedSch
            ] |> TupleSequence.Create expectedSch
                
        let rpipe : PipeStage = 
            "rtap" *> IdentityPipe 
        
        let lpipe : PipeStage =  "ltap" *>  IdentityPipe  <**> (ExplodeInner ("values",["name_idL"],"value") (["name_idR"],"name"), rpipe)
            
        let ltap = Tap.Create(fun () -> leftSeq) 
        let rtap = Tap.Create(fun () -> nameMapSeq) 
        
        let taps = ["ltap",ltap;"rtap",rtap]
        let sink = new CollectorSink()                        
        let tapped = RunPipes sink lpipe taps                                                                            
        let result = sink.GetSeq            
                            
        assertTseqEq expectedSeq result
        
        ()
                
        
    [<Fact>]
    let testRunInnerPipe () =
                
        let innerS = testSchemaCreate [IntElement;StringElement] ["inner1";"inner2"]        
        let outerS = testSchemaCreate [FloatElement;SequenceElement innerS] ["outer1";"foo"]
        let exSI = testSchemaCreate [IntElement] ["qux"]
        let exS = testSchemaCreate [SequenceElement exSI ;FloatElement;SequenceElement innerS] ["bar";"outer1";"foo"]
                
        let in1 = [Int 1L;String "a"] |> Tuple.Create innerS
        let in2 = [Int 2L;String "b"] |> Tuple.Create innerS
        let in3 = [Int 3L;String "c"] |> Tuple.Create innerS
        let inE1 = [Int 1L] |> Tuple.Create exSI
        let inE2 = [Int 2L] |> Tuple.Create exSI
        let inE3 = [Int 3L] |> Tuple.Create exSI

        let ts1 = seq [in1;in2;in3] |> TupleSequence.Create innerS
        let ts2 = seq [in3] |> TupleSequence.Create innerS
        
        let tsE1 = seq [inE1;inE2;inE3] |> TupleSequence.Create exSI
        let tsE2 = seq [inE3] |> TupleSequence.Create exSI
        
        let out1 = [Float 1.0;Tuples ts1] |> Tuple.Create outerS
        let out2 = [Float 2.0;Tuples ts2] |> Tuple.Create outerS        
        let ex1 = [Tuples tsE1;Float 1.0;Tuples ts1] |> Tuple.Create exS
        let ex2 = [Tuples tsE2;Float 2.0;Tuples ts2] |> Tuple.Create exS
        let ex = seq [ex1;ex2] |> TupleSequence.Create exS
                        
        let tap = Tap.Create(fun () -> seq [out1;out2] |> TupleSequence.Create outerS) 
        let innerPipeStages = MapPipe.Create ("getinner1",                                                  
                                                  (fun (t:Tuple) -> [t.GetDatum "inner1"] |> Tuple.Create exSI),
                                                  orderPreserving,
                                                  (fun sch -> assertEqual innerS sch; exSI)) 
        
        let pipe = "tap" *> InnerPipe ("foo",(fun _ -> Column.Make("bar",SequenceElement exSI))) innerPipeStages
        let sink = new CollectorSink ()
        RunPipes sink pipe ["tap",tap]
        let res = sink.GetSeq
        assertTseqEq ex res        
        ()
        
    [<Fact>]
    let testPublishStage () = 
        let AddPipe fieldName diff = 
            let mapfun (tup:Tuple)= 
                let newval = Int ((tup.GetInt fieldName) + int64 diff)
                tup.UpdateValue fieldName newval
            MapPipe.Create ("mapadd",mapfun,orderPreserving,(fun x -> x))
                    
        // [100;101;102;103]-> x -> Add 10 -> main = [110;111;112;113]
        //                     ^-> Add 1 -> x-> Add 1000 -> spur1 = [1101;1102;1103;1104]
        //                     |            ^-> Add 100 -> InnerJoin [200;201;202;203] -> spur2 [201;202;203]
        //                     ^-> Add 200 -> InnerJoin [300;301;302;303] -> spur3 [300;301;302;303]
        
        let left = "left" *> IdentityPipe
        let right = "right" *> IdentityPipe
        let mainPipe = "main" *> IdentityPipe <%> "connector1" <*> AddPipe "val" 10
        let spur1 = "connector1" *> AddPipe "val" 1 <%> "connector2" <*> AddPipe "val" 1000
        let spur2 = "connector1" *> DropColumns ["m"] <*> AddPipe "val" 100 <**> (Join (JoinSmall,JoinInner) ["val"] ["val"],left)
        let spur3 = "connector2" *> DropColumns ["m"] <*> AddPipe "val" 200 <**> (Join (JoinSmall,JoinInner) ["val"] ["val"],right)
                       
        let genseq nm lst = 
            let innerSch = Schema.Create [sc3 ("ival"+ nm,IntElement,Int -1L)]
            let sch = Schema.Create [sc3 ("val",IntElement,Int -1L); sc2 (nm,SequenceElement innerSch)]
            let inner = TupleSequence.Create innerSch (innerSch.DefaultTuple |> Seq.singleton)
            let tn n = Tuple.Create sch [Int (int64 n); inner |> Tuples]
            Seq.map tn lst |> TupleSequence.Create sch
        let mseq = [100;101;102;103] |> genseq "m"
        let lseq = [200;201;202;203] |> genseq "l"
        let rseq = [300;301;302;303] |> genseq "r"
        
        let exMain = [110;111;112;113] |> genseq "m"
        let exSpur1 = [1101;1102;1103;1104] |> genseq "m"
        let exSpur2 = [200;201;202;203] |> genseq "l"
        let exSpur3 = [301;302;303] |> genseq "r"
        
        let ltap = Tap.Create (fun () -> lseq) 
        let rtap = Tap.Create (fun () -> rseq) 
        let mtap = Tap.Create (fun () -> mseq) 
        let mainSink = new CollectorSink () :> ISink 
        let spurSink1 = new CollectorSink () :> ISink 
        let spurSink2 = new CollectorSink () :> ISink 
        let spurSink3 = new CollectorSink () :> ISink 
        MultiRunPipes [ mainSink , mainPipe;  spurSink1,spur1; spurSink2 ,spur2; spurSink3,spur3] 
                      ["left",ltap;"right",rtap;"main",mtap]
        let resMain = (mainSink :?> CollectorSink).GetSeq
        let resSpur1 = (spurSink1:?> CollectorSink).GetSeq
        let resSpur2 = (spurSink2 :?> CollectorSink).GetSeq
        let resSpur3 = (spurSink3 :?> CollectorSink).GetSeq
        
        assertTseqEq exMain resMain
        assertTseqEq exSpur1 resSpur1
        assertTseqEq exSpur2 resSpur2
        assertTseqEq exSpur3 resSpur3                               
        ()
        
    [<Fact>]
    let testPublishNonTree () = 
        let AddPipe fieldName diff = 
            let mapfun (tup:Tuple)= 
                let newval = Int ((tup.GetInt fieldName) + int64 diff)
                tup.UpdateValue fieldName newval
            MapPipe.Create ("mapadd",mapfun,orderPreserving,(fun x -> x))
                    
        // [100;101;102;103]->  Add 1 -> x-> InnerJoin x ->  [101;102;103]
        //                               ^-> Add 100 --^                 
        let spur = "connector1" *> AddPipe "val" 1 
        let mainPipe = "main" *> IdentityPipe <%> "connector1" <**> (Join (JoinSmall,JoinInner) ["val"] ["val"],spur)
                
        let sch = Schema.Create [sc3 ("val",IntElement,Int -1L)]
        let genseq lst = 
            let tn n = Tuple.Create sch [Int (int64 n)]
            Seq.map tn lst |> TupleSequence.Create sch
        let mseq = [100;101;102;103] |> genseq                
        let exMain = [101;102;103] |> genseq
                        
        let mtap = Tap.Create (fun () -> mseq) 
        let mainSink = new CollectorSink () :> ISink 
        
        MultiRunPipes [ mainSink , mainPipe]  ["main",mtap]
        let resMain = (mainSink :?> CollectorSink).GetSeq
                
        assertTseqEq exMain resMain        
        ()
        
    [<Fact>]
    let testPublishNonTreeBlockingJoin () = 
        let AddPipe fieldName diff = 
            let mapfun (tup:Tuple)= 
                let newval = Int ((tup.GetInt fieldName) + int64 diff)
                tup.UpdateValue fieldName newval
            MapPipe.Create ("mapadd",mapfun,orderPreserving,(fun x -> x))
                    
        // [100;101;102;103]-> x ->         InnerJoin x ->  [101;102;103]
        //                     ^-> Add 1   -> y  -> --^ 
        //                                    ^ -> Add 1 -> [102;103;104;105]
        let spur = "connector1" *> AddPipe "val" 1  <%> "connector2"
        let spur2 = "connector2" *> AddPipe "val" 1 
        let mainPipe = "main" *> IdentityPipe  <%> "connector1" <**> (Join (JoinSmall,JoinInner) ["val"] ["val"],spur)

        let sch = Schema.Create [sc3 ("val",IntElement,Int -1L)]
        let genseq lst = 
            let tn n = Tuple.Create sch [Int (int64 n)]
            Seq.map tn lst |> TupleSequence.Create sch
        let mseq = [100;101;102;103] |> genseq                
        let exMain = [101;102;103] |> genseq
        let exSpur = [102;103;104;105] |> genseq
                                
        let mtap = Tap.Create (fun () -> mseq) 
        let mainSink = new CollectorSink () :> ISink 
        let spur2Sink = new CollectorSink () :> ISink 
        
        MultiRunPipes [ mainSink , mainPipe ; spur2Sink,spur2]  ["main",mtap]
        let resMain = (mainSink :?> CollectorSink).GetSeq
        let resSpur = (spur2Sink :?> CollectorSink).GetSeq
                
        assertTseqEq exMain resMain        
        assertTseqEq exSpur resSpur
        ()
        
           
    // this follows a pretty common pattern --
    // taps a,b and sinks l,r and publish points x,y
    //
    //     a      b
    //     v      v
    //     |      |
    //     add2  add1
    //     |      |
    //     J <----$x
    //     |      |
    //     add10 add100
    //     |      |
    //     $y---> M
    //     |      |
    //     v      v
    //     l      r
    [<Fact>]
    let testPublishLadder() = 
        let AddPipe fieldName diff = 
            let mapfun (tup:Tuple)= 
                let newval = Int ((tup.GetInt fieldName) + int64 diff)
                tup.UpdateValue fieldName newval
            MapPipe.Create ("mapadd",mapfun,orderPreserving,(fun x -> x))
                            
        let spurX = "x" *> IdentityPipe
        let spurY = "y" *> IdentityPipe
        let a = "a" *> AddPipe "val" 2 <**> (Join (JoinSmall,JoinInner) ["val"] ["val"],spurX) <*> AddPipe "val" 10 <%> "y"
        let b = "b" *> AddPipe "val" 1 <%> "x" <*> AddPipe "val" 100 <**> (JoinCollateOrdered  ["val"],spurY) 
        
        let sch = Schema.Create [sc3 ("val",IntElement,Int -1L)]
        let genseq lst = 
            let tn n = Tuple.Create sch [Int (int64 n)]
            let key = SchemaKey.Create ([Ascending,"val"],true)
            Seq.map tn lst |> TupleSequence.CreateKeyed (sch.SetKey key)
        let aseq = [100;101;102;103;104] |> genseq                
        let bseq = [100;102;104;106] |> genseq
        let lEx = [113;115] |> genseq
        let rEx = [113;115;201;203;205;207] |> genseq
                                
        let atap = Tap.Create (fun () -> aseq) 
        let btap = Tap.Create (fun () -> bseq) 
        
        let lsink = new CollectorSink () :> ISink 
        let rsink = new CollectorSink () :> ISink 
        
        MultiRunPipes [ lsink, a; rsink,b]  ["a",atap;"b",btap]
        let lRes= (lsink:?> CollectorSink).GetSeq
        let rRes= (rsink:?> CollectorSink).GetSeq
                        
        assertTseqEq lEx lRes
        assertTseqEq rEx rRes
        ()
        
    
        
        
        
