open Printf

open Svg

open Utils
open Conll_types

(* ======================================================================================================================== *)
module Dep_node = struct
  type t = {
    id: Id.t;
    lines: (string * int) list;
    svg: Svg.t;
    width: int;
  }

  let id_to_string n = Id.to_string n.id

  let build id lines =
    let rec loop y = function
    | [] -> []
    | (text, font_size) :: tail ->
        let svg = Svg.text
          ~background:"#fedcba80"
          (* ~bounding_box:Color.black *)
          ~vertical_align: "top"
          ~text_anchor:"middle"
          ~font_size
          ~x:0 ~y ~text () in
        svg :: (loop (y + Svg.height svg) tail) in
    let svg_list = loop 0 lines in
    let svg = Svg.group svg_list in
    let width = List.fold_left (fun acc item -> max acc (Svg.width item)) 0 svg_list in
    { id; lines; svg; width }
end

(* ======================================================================================================================== *)
module Dep_edge = struct
(* [label] is the type of edges labels*)
  type label = string

  type t = {
    src: Id.t;
    tar: Id.t;
    label: label;
    bottom: bool;
    level: int;
    src_shift: int;
    tar_shift: int;
  }

  let build src tar label bottom = { src; tar; label; bottom; level=0; src_shift=0; tar_shift=0 }

  let compare_levels e1 e2 = compare e1.level e2.level

  (* return a boolean true iff edge1 is drawn under edge2 *)
  let under edge1 edge2 =
    let (l1, r1) = Id.min_max edge1.src edge1.tar
    and (l2, r2) = Id.min_max edge2.src edge2.tar in
    ((compare r1 l2) > 0 && (compare r1 r2) < 0) || ((compare l1 l2) > 0 && (compare r1 r2) = 0)

  let parallel edge1 edge2 =
    ((edge1.src = edge2.src) && (edge1.tar = edge2.tar)) ||
    ((edge1.src = edge2.tar) && (edge1.tar = edge2.src))

  let under_compare edge1 edge2 =
    if edge1.bottom <> edge2.bottom
    then 0
  else
    if under edge1 edge2
    then -1
  else
    if under edge2 edge1
    then 1
  else 0

  let set_shift edge id value =
    if edge.src = id
    then {edge with src_shift = value}
    else if edge.tar = id
    then {edge with tar_shift = value}
    else failwith "Invalid_argument in set_shift"

  let low edge = Id.min edge.src edge.tar
  let high edge = Id.max edge.src edge.tar
end

(* ======================================================================================================================== *)
module Draw_dep = struct
  let up y height x_init x_final =
    let delta_x = x_final - x_init in
    let d =
      if delta_x > 12
      then
        sprintf
          "M %d %d v %d a6,6 0 0,1 6,-6 h %d a6,6 0 0,1 6,6 v %d"
          x_init y (height-6) (delta_x-12) (-height)
    else
      if delta_x < -12
      then
        sprintf
          "M %d %d v %d a6,6 0 0,0 -6,-6 h %d a6,6 0 0,0 -6,6 v %d"
          x_init y (height-6) (delta_x+12) (-height)
    else failwith "|delta_x| must be > 12" in
    Svg.path ~stroke_width:2 ~arrow:true ~d ()

    end

(* ======================================================================================================================== *)
module Dep = struct
  type t = {
    nodes: Dep_node.t list;
    edges: Dep_edge.t list;
  }

  let dump t =
    Printf.printf "======= Nodes =======\n";
    Printf.printf "%s\n" (String.concat ", " (List.map Dep_node.id_to_string t.nodes));
    Printf.printf "======= Edges =======\n";
    List.iter
      (fun e -> Printf.printf "%s -[%s]-> %s (%s) ==> level=%d, src_shift=%d, tar_shift=%d\n"
        (Id.to_string e.Dep_edge.src)
        e.Dep_edge.label
        (Id.to_string e.Dep_edge.tar)
        (if e.Dep_edge.bottom then "down" else "up")
        e.Dep_edge.level
        e.Dep_edge.src_shift
        e.Dep_edge.tar_shift
      ) t.edges;
    Printf.printf "=====================\n%!";
    ()




  let compute_levels edge_list =
    let sorted_edge_list = List.sort Dep_edge.under_compare edge_list in
    let rec loop acc = function
    | [] -> acc
    | edge :: tail ->
      let ulevel = List.fold_left
        (fun acc_level e ->
            if Dep_edge.parallel e edge
            then e.Dep_edge.level
          else
            if Dep_edge.under e edge
            then max acc_level e.Dep_edge.level
          else acc_level
        ) 0 acc in
        loop ({edge with Dep_edge.level = ulevel+1} :: acc) tail
    in loop [] sorted_edge_list


  let compute_shifts nodes edge_list_with_levels =
    List.fold_left
      (fun acc {Dep_node.id} ->
        let (left_incident, tmp) = List.partition (fun e -> Dep_edge.high e = id) acc in
        let (right_incident, non_incident) = List.partition (fun e -> Dep_edge.low e = id) tmp in

        let (left_below,left_above) = List.partition (fun e -> e.Dep_edge.bottom) left_incident
        and (right_below,right_above) = List.partition (fun e -> e.Dep_edge.bottom) right_incident in

        let sorted_left_above = List.sort Dep_edge.compare_levels left_above
        and sorted_right_above = List.sort (fun e1 e2 -> -(Dep_edge.compare_levels e1 e2)) right_above
        and sorted_left_below = List.sort Dep_edge.compare_levels left_below
        and sorted_right_below = List.sort (fun e1 e2 -> -(Dep_edge.compare_levels e1 e2)) right_below in

        let above = sorted_left_above @ sorted_right_above
        and below = sorted_left_below @ sorted_right_below in

        let len_above = List.length above
        and len_below = List.length below in

        let above_with_shift = List.mapi (fun i edge -> Dep_edge.set_shift edge id (2*i+1-len_above)) above
        and below_with_shift = List.mapi (fun i edge -> Dep_edge.set_shift edge id (2*i+1-len_below)) below in

        above_with_shift @ below_with_shift @ non_incident
      ) edge_list_with_levels nodes

  let h_pad = 20

  let to_svg file t =
    let (total_width, moved_pack_list, pos_assoc) =
      List.fold_left
        (fun (left, pack_acc, pos_acc) node ->
          let w = node.Dep_node.width in
          (
            left+w+h_pad,
            (Svg.translate (left+w/2,0) node.Dep_node.svg) :: pack_acc,
            (node.Dep_node.id, left+w/2) :: pos_acc
          )
        ) (h_pad, [], []) t.nodes in

    let words = Svg.group (moved_pack_list) in

    let edges = Svg.group
      (List.map
        (fun edge ->
          let src_pos = List.assoc edge.Dep_edge.src pos_assoc
          and tar_pos = List.assoc edge.Dep_edge.tar pos_assoc in
          let y_pos = (-15 * edge.Dep_edge.level)+12 in
          let x_init = src_pos + 3*edge.Dep_edge.src_shift
          and x_final = tar_pos + 3*edge.Dep_edge.tar_shift in
          Svg.group [
            Svg.text ~font_size: 10 ~x:((x_init+x_final)/2) ~y:(y_pos-15) ~text:edge.Dep_edge.label ~background: "#abcdef" ();
            Draw_dep.up 0 y_pos x_init x_final;
          ]
        ) t.edges
      ) in

    let full = Svg.group [
      Svg.translate (0,200) words;
      Svg.translate (0,200) edges;
    ] in

  Svg.save file total_width 480 full











  (* let to_svg file t =
    let pack_width_assoc = List.map (fun node -> (node.Dep_node.id, Draw_dep.pack node.Dep_node.lines)) t.nodes in

    let (total_width, moved_pack_list, pos_assoc) =
      List.fold_left
        (fun (left, pack_acc, pos_acc) (id, (pack, w)) ->
          (
            left+w+h_pad,
            (Svg.translate (left+w/2,0) pack) :: pack_acc,
            (id, left+w/2) :: pos_acc
          )
        ) (h_pad, [], []) pack_width_assoc in

    let words = Svg.group (moved_pack_list) in

    let edges = Svg.group
      (List.map
        (fun edge ->
          let src_pos = List.assoc edge.Dep_edge.src pos_assoc
          and tar_pos = List.assoc edge.Dep_edge.tar pos_assoc in
          let y_pos = (-15 * edge.Dep_edge.level)+12 in
          let x_init = src_pos + 3*edge.Dep_edge.src_shift
          and x_final = tar_pos + 3*edge.Dep_edge.tar_shift in
          Svg.group [
            Svg.text ~font_size: 10 ~x:((x_init+x_final)/2) ~y:(y_pos-15) ~text:edge.Dep_edge.label ~background: "#abcdef" ();
            Draw_dep.up 0 y_pos x_init x_final;
          ]
        ) t.edges
      ) in

    let full = Svg.group [
      Svg.translate (0,200) words;
      Svg.translate (0,200) edges;
    ] in

  Svg.save file total_width 480 full *)
end
