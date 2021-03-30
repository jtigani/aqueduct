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

namespace Aqueduct.Test

open Xunit

module Utils =
    open Aqueduct.Primitives
    
    let testAssert fCheck fAssert a =
        if (fCheck a) then
            ()
        else
            fAssert a
            failwith "unexpected."

    let assertTrue a =
        testAssert (fun a -> true) Assert.True a
    let assertFalse a =
        testAssert (fun a -> false) Assert.False a
        
    let assertEqual a b =
        testAssert (fun (a,b) -> a = b) Assert.Equal (a,b)
        
    let assertReferenceEqual a b =
        testAssert (fun (a,b) -> a = b) Assert.Same (a,b)
        assertTrue (System.Object.ReferenceEquals(a,b))

    let assertNotEqual a b =
        testAssert (fun (a,b) -> a <> b) Assert.NotEqual (a,b)

    

    let assertContains eid lst =
        testAssert (fun (eid,lst) -> (List.exists(fun id -> id.Equals(eid)) lst)) Assert.Contains (eid,lst)

    let assertNotContains eid lst =
        testAssert
            ((fun (eid,lst) -> (List.exists(fun id -> id.Equals(eid)) lst)) >> not)
            Assert.DoesNotContain (eid,lst)

    let assertThrows f typ =
        let inheritsFrom (typ:System.Type)  obj = typ.IsAssignableFrom(obj.GetType())
        try
            f ()
            Assert.True(false)
        with
           | _ as ex ->
                if (inheritsFrom typ ex) then
                    ()
                else
                    Assert.True(false)
                    
    let rec assertTseqEq (ex:TupleSequence) (res:TupleSequence) =        
        let lstEx = List.ofSeq ex.tseq
        let lstRes = List.ofSeq res.tseq
        assertEqual (List.length lstEx) (List.length lstRes)                
        List.iter2 (fun e r -> assertSchemaEqual e.schema r.schema) lstEx lstRes
        if ex.Schema.Ordering.Length <> 0 then
            List.iter2 (fun e r -> List.iter2 assertDatumEq e.values r.values) lstEx lstRes
        else
            // since this isn't ordered, don't assume a particular ordering.
            // instead, make sure that for each tuple in the result list, there is a tuple
            // in the expected list with datums that match it exactly. Likewise, check each tuple in the
            // expected list has a tuple in the result list with matching data
            let check leftList rightTup =
                let found = leftList |> List.tryFind (fun leftTup -> List.forall2 (fun dl dr -> dl = dr) leftTup.values rightTup.values)
                assertTrue (found <> None)
            lstRes |> List.iter (check lstEx)
            lstEx |> List.iter (check lstRes)
            
    and assertDatumEq (ex:Datum) (res:Datum) = 
        let epsilon = 0.001
        assertEqual ex.Typeof res.Typeof
        match ex,res with
        | TuplesDatum exSeq, TuplesDatum resSeq -> assertTseqEq exSeq resSeq
        | FloatDatum f1, FloatDatum f2 -> Xunit.Assert.InRange(f2,f1-epsilon,f1+epsilon) // floating point sometimes looses precision
        | _ -> assertEqual ex res
    and assertSchemaEqual (lsch:Schema) (rsch:Schema) =
        assertEqual lsch.Key rsch.Key
        List.iter2 (fun (e:SchemaElement) (r:SchemaElement) -> 
                            match e,r with
                            | SequenceElement etups, SequenceElement rtups -> assertSchemaEqual etups rtups
                            | (_,_) -> assertEqual e r) lsch.Elements rsch.Elements
        List.iter2 (fun (e:string) (r:string) -> assertEqual e r) lsch.Names rsch.Names
        List.iter2 (fun (e:Datum) (r:Datum) -> assertDatumEq e r) lsch.DefaultValues rsch.DefaultValues
        assertEqual lsch rsch // in case we forgot anything...
        