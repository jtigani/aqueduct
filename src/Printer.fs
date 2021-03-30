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

/// Creates pipesystem graphs
module public Printer  =
    open Aqueduct.Primitives
    open Aqueduct.Pipes
    
    let PrintPipeGraph (writer:System.IO.TextWriter) (inputStages:(ISink * PipeStage) list) (taps:(string *ITap) list) : unit =
        writer.WriteLine("digraph pipes {")
        
        // A PublishStage and SubscribeStage are turned into a single stage (whichever one was first seen)
        // based on their name.
        
        let dotPublishStageNameHash = new System.Collections.Generic.Dictionary<string,PipeStage>()
        let dotPublishStage (stage:PipeStage) =
            match stage with
            | SubscribeStage name | PublishStage (name,_) -> if dotPublishStageNameHash.ContainsKey(name) then
                                                                 dotPublishStageNameHash.[name]
                                                             else
                                                                 dotPublishStageNameHash.[name] <- stage
                                                                 stage
            | s -> s
             
        // Generate a unique valid DOT node name for a stage.
            
        let dotStageNameHash = new System.Collections.Generic.Dictionary<PipeStage,string>()
        let dotStageNameExists (stage:PipeStage) =
            let stage = dotPublishStage stage
            dotStageNameHash.ContainsKey(stage)
        let dotStageName (stage:PipeStage) =
            let stage = dotPublishStage stage
            if dotStageNameExists stage then
                dotStageNameHash.[stage]
            else
                let value = sprintf "%d" dotStageNameHash.Count
                dotStageNameHash.[stage] <- value
                value  
        
        // Quote a string that will be used in a DOT file in a string (i.e. between double quotes).
        
        let dotQuotedString (str:string) = str.Replace("\"", "\\\"").Replace("\n", "\\n")
        
        // Generate a node description.
        
        let genNode stage =
            let nodeAttrs = System.String.Format(",label=\"{0}\"", GetStageName stage |> dotQuotedString)
            writer.Write(System.String.Format("{0} [", dotStageName stage))
            match stage with
            | SubscribeStage name -> writer.Write(System.String.Format("shape=pentagon,peripheries=2{0}];\n", nodeAttrs))
            | NormalStage (pipe,next) -> writer.Write(System.String.Format("shape=box{0}];\n", nodeAttrs))
            | JoinStage (pipe,left,right) -> writer.Write(System.String.Format("shape=invtrapezium{0}];\n", nodeAttrs))
            | PublishStage (name,next) -> writer.Write(System.String.Format("shape=polygon,sides=6,rotation=90,peripheries=2{0}];\n", nodeAttrs))
        
        // Generate the vertice between two nodes.
        
        let genVert stage =
            match stage with
            | SubscribeStage name -> ()
            | NormalStage (pipe,next) -> writer.Write(System.String.Format("{0} -> {1};\n", dotStageName next, dotStageName stage))                                        
            | JoinStage (pipe,left,right) -> writer.Write(System.String.Format("{0} -> {1};\n", dotStageName left, dotStageName stage))
                                             writer.Write(System.String.Format("{0} -> {1};\n", dotStageName right, dotStageName stage))
            | PublishStage (name,next) -> writer.Write(System.String.Format("{0} -> {1};\n", dotStageName next, dotStageName stage))         
    
        // Generate the description for a graph.
        
        let rec genGraph stage =
            genNode stage
            match stage with
            | SubscribeStage name -> ()
            | NormalStage (pipe,next) -> genGraph next                                        
            | JoinStage (pipe,left,right) -> genGraph left
                                             genGraph right
            | PublishStage (name,next) -> genGraph next
            genVert stage

        // For each graph in the pipe system, describe its sink, the graph itself, and its connection to the sink.
            
        inputStages |> List.iteri (fun i (sink,stg) ->
                                      writer.Write(System.String.Format("sink{0} [shape=invtriangle,peripheries=2,label=\"\\\"{1}\\\"\"];\n", box i, sink.Name() |> dotQuotedString))
                                      genGraph stg
                                      writer.Write(System.String.Format("{0} -> sink{1};\n", dotStageName stg, box i))
                                  )
        writer.WriteLine("}")
        
   