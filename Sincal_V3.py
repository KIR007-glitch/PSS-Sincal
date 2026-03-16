import pandas as pd
from collections import defaultdict, deque

#1) MOCK TERMINAL TABLE

terminal_file_path = "Terminal.xlsx"
terminal_df = pd.read_excel(terminal_file_path, sheet_name = "Terminal")
terminal_2_xl = terminal_df.to_excel("Terminal_reconstructed.xlsx")


#print(terminal_df)

### Cleaning the required columns for the phase table
##terminal_df["Node 1"] = terminal_df["Node 1"].astype(str).str.strip()
##terminal_df["Con. Type"] = terminal_df["Con. Type"].astype(str).str.strip().str.upper()



##terminal_data = [
##    {"Node 1": "A", "Element Name": "Infeeder 1", "Configuration Type": "L123"},
##
##    {"Node 1": "E", "Element Name": "Load 1", "Configuration Type": "L123"},
##    
##    {"Node 1": "A", "Element Name": "Line_AB", "Configuration Type": "L123"},
##    {"Node 1": "B", "Element Name": "Line_AB", "Configuration Type": "L123"},
##
##    {"Node 1": "B", "Element Name": "Line_BC", "Configuration Type": "L123"},
##    {"Node 1": "C", "Element Name": "Line_BC", "Configuration Type": "L123"},
##
##    {"Node 1": "D", "Element Name": "Line_CD", "Configuration Type": "L123"},
##    {"Node 1": "C", "Element Name": "Line_CD", "Configuration Type": "L123"},
##
##    {"Node 1": "C", "Element Name": "Line_CE", "Configuration Type": "L12"},
##    {"Node 1": "E", "Element Name": "Line_CE", "Configuration Type": "L12"},
##
##    {"Node 1": "E", "Element Name": "Line_EF", "Configuration Type": "L123"},   # intentionally wrong
##    {"Node 1": "F", "Element Name": "Line_EF", "Configuration Type": "L123"},
##
##    {"Node 1": "C", "Element Name": "Line_CG", "Configuration Type": "L1"},
##    {"Node 1": "G", "Element Name": "Line_CG", "Configuration Type": "L1"},
##
##    {"Node 1": "not_found", "Element Name": "Line_XY", "Configuration Type": "L3"},
##    {"Node 1": "J",         "Element Name": "Line_XY", "Configuration Type": "L3"},
##]
##
##terminal_df = pd.DataFrame(terminal_data)

#2) Function to check invalid nodes
def is_invalid_node(node):
    if pd.isna(node):
        return True

    node = str(node).strip().lower()
    invalid_names = {
        "not_found",
        "no data ",
        "not found_not found"
    }
    return node in invalid_names

#3) Build an element table from terminal table
def build_element_table(terminal_df):
    element_rows = []

    df = terminal_df.copy()
    df.columns = df.columns.str.strip()

    df = df[~df["Node 1"].apply(is_invalid_node)].copy()
    df["Node 1"] = df["Node 1"].astype(str).str.strip()
    df["Element Name"] = df["Element Name"].astype(str).str.strip()
    df["Con. Type"] = df[r"Con. Type"].astype(str).str.strip()
    df["Terminal_ID"] = pd.to_numeric(df["Terminal_ID"], errors="coerce")

    df = df.dropna(subset=["Terminal_ID"]).copy()
    df["Terminal_ID"] = df["Terminal_ID"].astype(int)

    grouped = df.groupby("Element Name", sort=False)

    for element_name, group in grouped:
        rows = group.to_dict("records")
        i = 0
        pair_num = 1

        while i < len(rows) - 1:
            row1 = rows[i]
            row2 = rows[i + 1]

            tid1 = row1["Terminal_ID"]
            tid2 = row2["Terminal_ID"]

            if tid2 == tid1 + 1:
                node1 = row1["Node 1"]
                node2 = row2["Node 1"]
                phase1 = row1["Con. Type"]
                phase2 = row2["Con. Type"]

                if phase1 == phase2 and node1 != node2:
                    element_rows.append({
                        "Element Instance": f"{element_name}__{pair_num}",
                        "Element Name": element_name,
                        "Terminal_ID_1": tid1,
                        "Terminal_ID_2": tid2,
                        "Node 1": node1,
                        "Node 2": node2,
                        "Phase": phase1
                    })
                    pair_num += 1
                    i += 2
                else:
                    print(f"Skipping {element_name} pair ({tid1}, {tid2})")
                    i += 2
            else:
                print(f"Unpaired row for {element_name}: Terminal_ID {tid1}")
                i += 1

        if i == len(rows) - 1:
            print(f"Unpaired last row for {element_name}: Terminal_ID {rows[i]['Terminal_ID']}")

    return pd.DataFrame(element_rows)



#4) Functions for phase convertions
def phase_to_set(phase):
    phase = str(phase).strip().upper()

    mapping = {
        "L1": {"L1"},
        "L2": {"L2"},
        "L3": {"L3"},
        "L12": {"L1", "L2"},
        "L21": {"L1", "L2"},
        "L23": {"L2", "L3"},
        "L32": {"L2", "L3"},
        "L13": {"L1", "L3"},
        "L31": {"L1", "L3"},
        "L123": {"L1", "L2", "L3"},
    }

    return mapping.get(phase, set())

def set_to_phase(phase_set):
    phase_set = set(phase_set)

    if phase_set == {"L1"}:
        return "L1"
    elif phase_set == {"L2"}:
        return "L2"
    elif phase_set == {"L3"}:
        return "L3"
    elif phase_set == {"L1", "L2"}:
        return "L12"
    elif phase_set == {"L2", "L3"}:
        return "L23"
    elif phase_set == {"L1", "L3"}:
        return "L13"
    elif phase_set == {"L1", "L2", "L3"}:
        return "L123"
    else:
        return None

#5) Building bidirectional graph and an edge lookup
    
def build_bidirectional_graph(element_df):
    graph = defaultdict(list)
    edge_lookup = defaultdict(list)

    for _, row in element_df.iterrows():
        n1 = str(row["Node 1"]).strip()
        n2 = str(row["Node 2"]).strip()
        
        element_instance = row["Element Instance"]
        element_name = row["Element Name"]
        terminal_id_1 = row["Terminal_ID_1"]
        terminal_id_2 = row["Terminal_ID_2"]                
        phase = row["Phase"]

        # build node-to-node graph
        graph[n1].append(n2)
        graph[n2].append(n1)

        # store all elements between this node pair
        key = tuple(sorted((n1, n2)))
        edge_lookup[key].append({
            "Element Instance": element_instance,
            "Element Name": element_name,
            "Terminal_ID_1": terminal_id_1,
            "Terminal_ID_2": terminal_id_2,
            "Phase": phase
        })

    return graph, edge_lookup


#6) Building a feeder tree
def build_feeder_tree(start_node, graph):
    queue = deque([start_node])
    visited = {start_node}
    parent = {start_node: None}
    bfs_order = []

    while queue:
        current = queue.popleft()
        bfs_order.append(current)

        for neighbor in graph.get(current, []):
            if neighbor not in visited:
                visited.add(neighbor)
                parent[neighbor] = current
                queue.append(neighbor)

    return parent, bfs_order

###7) Validate /Correct Phasing
def validate_and_correct_phasing(parent, bfs_order, edge_lookup):
    results = []

    for node in bfs_order:
        par = parent[node]

        if par is None:
            continue

        current_edge_key = tuple(sorted((par, node)))
        current_edge_data = edge_lookup[current_edge_key][0]

        current_element_instance = current_edge_data["Element Instance"]
        current_element_name = current_edge_data["Element Name"]
        current_terminal_id_1 = current_edge_data["Terminal_ID_1"]
        current_terminal_id_2 = current_edge_data["Terminal_ID_2"]
        current_phase = current_edge_data["Phase"]
        current_phase_set = phase_to_set(current_phase)

        grandparent = parent.get(par)

        if grandparent is None:
            results.append({
                "Upstream Element Instance": None,
                "Upstream Element Name": None,
                "Upstream Terminal_ID_1": None,
                "Upstream Terminal_ID_2": None,
                "Upstream Phase": None,
                
                "Downstream Element Instance": current_element_instance,
                "Downstream Element Name": current_element_name,
                "Downstream Terminal_ID_1": current_terminal_id_1,
                "Downstream Terminal_ID_2": current_terminal_id_2,
                "Downstream Phase": current_phase,
                "Status": "SOURCE_EDGE",
                "Suggested Phase": current_phase
            })
            continue

        upstream_edge_key = tuple(sorted((grandparent, par)))
        upstream_edge_data = edge_lookup[upstream_edge_key][0]

        upstream_element_instance = upstream_edge_data["Element Instance"]
        upstream_element_name = upstream_edge_data["Element Name"]
        upstream_terminal_id_1 = upstream_edge_data["Terminal_ID_1"]
        upstream_terminal_id_2 = upstream_edge_data["Terminal_ID_2"]
        upstream_phase = upstream_edge_data["Phase"]
        upstream_phase_set = phase_to_set(upstream_phase)

        if current_phase_set.issubset(upstream_phase_set):
            status = "VALID"
            suggested_phase = current_phase
        else:
            corrected_phase_set = current_phase_set.intersection(upstream_phase_set)

            if corrected_phase_set:
                suggested_phase = set_to_phase(corrected_phase_set)
                status = "INVALID_CORRECTABLE"
            else:
                suggested_phase = None
                status = "INVALID_NO_COMMON_PHASE"

        results.append({
            "Upstream Element Instance": upstream_element_instance,
            "Upstream Element Name": upstream_element_name,
            "Upstream Terminal_ID_1": upstream_terminal_id_1,
            "Upstream Terminal_ID_2": upstream_terminal_id_2,
            "Upstream Phase": upstream_phase,
            
            "Downstream Element Instance": current_element_instance,
            "Downstream Element Name": current_element_name,
            "Downstream Terminal_ID_1": current_terminal_id_1,
            "Downstream Terminal_ID_2": current_terminal_id_2,
            "Downstream Phase": current_phase,
            
            "Status": status,
            "Suggested Phase": suggested_phase
        })

    return pd.DataFrame(results)

##def validate_and_correct_phasing(parent, bfs_order, edge_lookup):
##    results = []
##
##    for node in bfs_order:
##        par = parent[node]
##
##        if par is None:
##            continue
##
##        current_edge_key = tuple(sorted((par, node)))
##        current_element = edge_lookup[current_edge_key]["Element Name"]
##        current_phase = edge_lookup[current_edge_key]["Phase"]
##        current_phase_set = phase_to_set(current_phase)
##
##        grandparent = parent.get(par)
##
##        if grandparent is None:
##            results.append({
##                "Upstream Element": None,
##                "Downstream Element": current_element,
##                "Upstream Phase": None,
##                "Downstream Phase": current_phase,
##                "Status": "SOURCE_EDGE",
##                "Suggested Phase": current_phase
##            })
##            continue
##
##        upstream_edge_key = tuple(sorted((grandparent, par)))
##        upstream_element = edge_lookup[upstream_edge_key]["Element Name"]
##        upstream_phase = edge_lookup[upstream_edge_key]["Phase"]
##        upstream_phase_set = phase_to_set(upstream_phase)
##
##        if current_phase_set.issubset(upstream_phase_set):
##            status = "VALID"
##            suggested_phase = current_phase
##        else:
##            corrected_phase_set = current_phase_set.intersection(upstream_phase_set)
##
##            if corrected_phase_set:
##                suggested_phase = set_to_phase(corrected_phase_set)
##                status = "INVALID_CORRECTABLE"
##            else:
##                suggested_phase = None
##                status = "INVALID_NO_COMMON_PHASE"
##
##        results.append({
##            "Upstream Element": upstream_element,
##            "Downstream Element": current_element,
##            "Upstream Phase": upstream_phase,
##            "Downstream Phase": current_phase,
##            "Status": status,
##            "Suggested Phase": suggested_phase
##        })
##
##    return pd.DataFrame(results)


#8) Run your functions

print("TERMINAL TABLE")
print(terminal_df)

element_df = build_element_table(terminal_df)

element_2_xl = element_df.to_excel("Element.xlsx")

print("ELEMENT TABLE")
print(element_df.head(20))

graph, edge_lookup = build_bidirectional_graph(element_df)

print("\nGRAPH SAMPLE")
for node, nbrs in list(graph.items())[:10]:
    print(node, "->", nbrs)

print("\nEDGE LOOKUP SAMPLE")
for k, v in list(edge_lookup.items())[:10]:
    print(k, "->", v)


start_node = "eo_circuit_13379029"
parent, bfs_order = build_feeder_tree(start_node, graph)

print("\nPARENT MAP")
print(parent)

print("\nBFS ORDER")
print(bfs_order)
##
results_df = validate_and_correct_phasing(parent, bfs_order, edge_lookup)

print("\nPHASE VALIDATION RESULTS")
results_excel = results_df.to_excel("Result.xlsx")
print(results_df)

    
