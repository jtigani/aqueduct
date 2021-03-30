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

module PrimitivesTests =
    open Aqueduct.Test.Utils    
    open Aqueduct.Primitives
    
    [<Fact>]
    let testSchema () = 
        let s1 = Schema.Make ["foo", IntElement; "bar", StringElement]
        let s2 = Schema.Make ["bar", StringElement];     
        let s3 = s1.DropColumns ["foo"]
        assertEqual s2 s3
        assertEqual s1 ((Schema.Make ["foo", IntElement]).Concat s3)
        
        let updateFun (comp:Column) = (comp.name, comp.SetName(comp.name + "2"))
        let s4 = Schema.Make ["foo2", IntElement; "bar2", StringElement]
        let s5 = s1.Columns |> List.map updateFun |> s1.Update       
        assertEqual s4 s5
        
        let s6 = Schema.Make ["foo", IntElement; "fooi", IntElement]
        let s7 = s1.Update ["bar", Column.Make("fooi", IntElement)]
        assertEqual s6 s7
        
        let s8 = Schema.Make ["fooi", IntElement; "foo", StringElement]
        let s9 = s1.Update ["foo", Column.Make("fooi", IntElement);
                            "bar", Column.Make("foo", StringElement)]
        assertEqual s8 s9
        
        ()
        
    