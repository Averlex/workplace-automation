"""
    Module performs some typed files parsing and processing. The output is a compressed and united file, stripped of
    anything what was considered unuseful.
    Basic approach - tree traversal
"""

import pandas as pd
from excel_operations import io, merger
import networkx as nx

if __name__ == "__main__":

    source_path = r""
    target_path = r""
    tables = io.read_xlsx_files(source_path)

    tables_dict = {}
    graphs = {}
    # Dropping the first column of each DataFrame + branches distribution
    # TODO: check if it's empty is required
    for table in tables:
        table = table.drop(table.columns[0], axis=1)
        # Extracting branches
        # TODO: some checks here too
        key = str(table.iloc[9, 0]).split(" ")[1].lower()
        splitted_key = key.split("-")
        short_key = ""
        for word in splitted_key:
            short_key += word[0]
        table["branch"] = short_key
        tables_dict[f"{short_key}"] = table

    def fill_first_col(table: pd.DataFrame, graph: nx.DiGraph) -> pd.DataFrame:
        # The first row with the data we want to parse
        # TODO: dynamic search here is needed
        start_row = 18
        level_col = table.columns.get_loc("header level")
        parent_row_indx = 0

        for row_num in range(start_row, len(table.index.tolist())):
            # Filling with new values
            if table.iloc[row_num, 0] == "":
                table.iloc[row_num, 0] = table.iloc[row_num - 1, 0]

            # Filling level values
            cell_value = str(table.iloc[row_num, 0]).lower()

            if table.iloc[row_num, 0] == table.iloc[row_num - 1, 0]:
                table.iloc[row_num, level_col] = table.iloc[row_num - 1, level_col]
                # Adding concurrent nodes
                graph.add_node(row_num, value=table.iloc[row_num, 2])
                graph.add_edge(parent_row_indx, row_num)

            elif "total" in cell_value or "res" in cell_value:
                table.iloc[row_num, level_col] = table.iloc[row_num - 1, level_col] - 1
                # Slicing by level name
                if "total" in cell_value and cell_value != "total:" or "res" in cell_value:
                    search_key = str(table.iloc[row_num, 0])[6:]
                    # "res" case contains headers with lowercased first letter
                    if "res" in cell_value:
                        search_key = search_key[0].upper() + search_key[1:-1]

                    for prev_row_num in range(row_num - 1, start_row - 1, -1):
                        section_start = prev_row_num
                        # The first occurrence ever
                        if table.iloc[prev_row_num, 0] == search_key:
                            # Looking up for the section beginning
                            for j in range(prev_row_num, start_row - 1, -1):
                                if table.iloc[j, 0] == table.iloc[prev_row_num, 0]:
                                    section_start = j
                                else:
                                    break
                            # Calculating header level
                            search_key_level = table.iloc[prev_row_num, table.columns.get_loc("header level")]
                            # Normal structure
                            if table.iloc[prev_row_num, 0] != table.iloc[prev_row_num - 1, 0]:
                                # Header difference should be not more than 1 level
                                unique_onodes = {}
                                for row_elem in table.iloc[prev_row_num:row_num].iterrows():
                                    # First occurrence of a parent header
                                    if (row_elem[1].loc["header level"] == search_key_level + 1) and (
                                            "total" not in str(row_elem[1].iloc[0]).lower()) and (
                                            "res" not in str(row_elem[1].iloc[0]).lower()) and (
                                            row_elem[1].iloc[0] not in unique_onodes.keys()):
                                        unique_onodes[row_elem[1].iloc[0]] = row_elem[0]
                                unique_nodes = [(section_start, indx) for indx in unique_onodes.values()]
                                graph.add_edges_from(unique_nodes)
                                break
                            # Mixed one
                            if prev_row_num + 1 != row_num:
                                table.iloc[prev_row_num + 1:row_num + 1, level_col] += 1
                                unique_onodes = {}
                                for row_elem in table.iloc[prev_row_num + 1:row_num].iterrows():
                                    # First occurrence of a parent header
                                    if (row_elem[1].loc["header level"] == search_key_level + 1) and (
                                            "total" not in str(row_elem[1].iloc[0]).lower()) and (
                                            "res" not in str(row_elem[1].iloc[0]).lower()) and (
                                            row_elem[1].iloc[0] not in unique_onodes.keys()):
                                        unique_onodes[row_elem[1].iloc[0]] = row_elem[0]
                                unique_nodes = [(section_start, indx) for indx in unique_onodes.values()]
                                graph.add_edges_from(unique_nodes)
                                break
                # "итого:" case
                else:
                    pass
            else:
                table.iloc[row_num, level_col] = table.iloc[row_num - 1, level_col] + 1
                # Adding the concurrent node
                graph.add_node(row_num, value=table.iloc[row_num, 0])
                parent_row_indx = row_num

            continue

        return table

    max_levels = []
    min_levels = []

    # Applying to each of the table we've got
    for key in tables_dict.keys():
        # Additional column for structure flattening
        tables_dict[key]["header level"] = 0
        graphs[key] = nx.DiGraph()
        graphs[key].name = key
        tables_dict[key] = fill_first_col(tables_dict[key], graphs[key])

        # Validating max and min levels
        min_levels.append(min(tables_dict[key]["header level"].values.tolist()))
        max_levels.append(max(tables_dict[key]["header level"].values.tolist()))

    if min(min_levels) < 0:
        raise ValueError(f"Errors in structure parsing: min level = {min(min_levels)}")

    level_count = max(max_levels)

    new_tables_dict = {}
    header_row_index = 14
    # Flattening the table
    for key in tables_dict.keys():
        new_tables_dict[key] = tables_dict[key][["филиал"]]
        # 1st column of interest
        new_tables_dict[key].loc[:, str(tables_dict[key].iloc[header_row_index, 2])] = ""
        # 2nd column of interest
        new_tables_dict[key].loc[:, str(tables_dict[key].iloc[header_row_index, 3])] = ""
        # 3rd column of interest
        new_tables_dict[key].loc[:, str(tables_dict[key].iloc[header_row_index, 4])] = ""
        # 4th column of interest
        new_tables_dict[key].loc[:, str(tables_dict[key].iloc[header_row_index, 11])] = ""
        # 5th column of interest
        new_tables_dict[key].loc[:, str(tables_dict[key].iloc[header_row_index, 16])] = ""

        # Adding columns for a flattened structure
        additions = []
        for i in range(level_count):
            additions.append(f"Subdivision {i}")
        additions[0] = "Category"
        new_tables_dict[key].loc[:, additions] = ""

        start_row = 18  # The first row to parse in the source table
        target_end_row = 0  # The last actual row in the result table
        for row_num in range(start_row, len(tables_dict[key].index.tolist())):
            # Might be empty spaces
            cell_value = tables_dict[key].iloc[row_num, 2].replace(" ", "")
            if cell_value != "":
                # Base values
                new_tables_dict[key].iloc[target_end_row, 1] = tables_dict[key].iloc[row_num, 2]
                new_tables_dict[key].iloc[target_end_row, 2] = int(tables_dict[key].iloc[row_num, 3])
                new_tables_dict[key].iloc[target_end_row, 3] = float(tables_dict[key].iloc[row_num, 4])
                new_tables_dict[key].iloc[target_end_row, 4] = float(tables_dict[key].iloc[row_num, 11])
                new_tables_dict[key].iloc[target_end_row, 5] = tables_dict[key].iloc[row_num, 16]
                # Structure
                start_col = 6

                # Leaf and its closest parent
                position = tables_dict[key].iloc[row_num, 2]
                super_position = tables_dict[key].iloc[row_num, 0]

                # Initialize a list to store the nodes from target to root
                parent_nodes = []
                # Perform a DFS traversal to find nodes from target to root
                current_node = row_num
                while current_node is not None:
                    parent_nodes.append(current_node)
                    predecessors = list(graphs[key].predecessors(current_node))
                    if predecessors:
                        current_node = predecessors[0]
                    else:
                        current_node = None
                parent_nodes.remove(row_num)

                # if matching_leaf is None:
                if len(parent_nodes) == 0:
                    raise ValueError("No matches were found for leaves")

                # Taking the values of actual predecessors
                parent_nodes.sort(reverse=False)
                parent_nodes = [graphs[key].nodes[node]["value"] for node in parent_nodes]
                if len(parent_nodes) > len(additions):
                    raise ValueError(f"Too much parent nodes: parents_len = {len(parent_nodes)}, levels = {len(additions)} ")
                diff = len(additions) - len(parent_nodes)
                parent_nodes += ["" for i in range(diff)]
                new_tables_dict[key].iloc[target_end_row, start_col:] = parent_nodes

                # Iteration
                target_end_row += 1

        # Dropping empty rows
        new_tables_dict[key] = new_tables_dict[key].drop(index=[extra_row for extra_row in range(target_end_row, len(new_tables_dict[key].index.tolist()))])

    res = {"Branch + Other branch": merger.concat_tables([new_tables_dict["ю"], new_tables_dict["юз"]], "v", True),
           "One more branch + Another one": merger.concat_tables([new_tables_dict["ц"], new_tables_dict["сз"]], "v", True),
           "Something else + A bit more of the same": merger.concat_tables([new_tables_dict["св"], new_tables_dict["в"]], "v", True)}
    for value in res.values():
        value.reset_index(drop=True, inplace=True)

    all_res = (merger.concat_tables(list(new_tables_dict.values()), "v", drop_indices=True))
    all_res.reset_index(drop=True, inplace=True)

    io.form_new_xlsx(new_tables_dict, target_path, file_name="By branch", index=True)
    io.form_new_xlsx(res, target_path, file_name="Branches merged", index=True)
    io.form_new_xlsx(all_res, target_path, file_name="Branches pivots", index=True)

    def drop_images(key: str):
        """
        Tree visualization
        :param key: a key for searching
        :return: nothing
        """
        import matplotlib.pyplot as plt
        # Draw the tree
        pos = nx.planar_layout(graphs[key])  # Specify the tree layout (hierarchical layout)
        # Get 'value' attribute as node labels
        node_labels = nx.get_node_attributes(graphs[key], 'value')
        nx.draw(graphs[key], pos, with_labels=False, node_size=500)
        # Add 'value' labels to the nodes
        nx.draw_networkx_labels(graphs[key], pos, labels=node_labels, font_size=3, font_color='black', font_weight='bold')
        plt.show()

        return
