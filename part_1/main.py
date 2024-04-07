import csv
import heapq
import os
import sys
from collections import defaultdict
import pandas as pd


def load_neighbours_to_graph(data):
    neighbours = defaultdict(set)
    for _, row in data.iterrows():
        neighbours[row['Creditor']].add(row['Debtor'])
        neighbours[row['Debtor']].add(row['Creditor'])

    return neighbours


def find_graphs(neighbours):
    visited = set()
    graphs = []
    for node in neighbours.keys():
        if node not in visited:
            node_to_visit = [node]
            current_visited_nodes = set()
            while node_to_visit:
                current_node = node_to_visit.pop()
                if current_node not in visited:
                    visited.add(current_node)
                    current_visited_nodes.add(current_node)
                    node_to_visit.extend(neighbours[current_node] - current_visited_nodes)
            graphs.append(current_visited_nodes)
    return graphs


def split_balances(graphs, balance):
    debtors = []
    creditors = []

    for graph in graphs:
        debtor_dict = {person: person_balance for person, person_balance in balance.items() if
                       person in graph and person_balance > 0}
        creditor_dict = {person: -person_balance for person, person_balance in balance.items() if
                         person in graph and person_balance < 0}
        debtors.append(debtor_dict)
        creditors.append(creditor_dict)

    return debtors, creditors


def calculate_transfers(debtors, creditors):
    if len(debtors) != len(creditors):
        raise ValueError("Length of debtors and creditors lists must be equal")

    transfers = []

    for debtors, creditors in zip(debtors, creditors):
        for debtor_person, debtor_balance in list(debtors.items()):
            if debtor_balance in creditors.values():
                creditor_person = [person for person, balance in creditors.items() if balance == debtor_balance][0]
                creditors.pop(creditor_person)
                debtors.pop(debtor_person)
                transfers.append((creditor_person, debtor_person, int(debtor_balance)))

        creditors_heap = [(-amount, creditor) for creditor, amount in creditors.items()]
        heapq.heapify(creditors_heap)
        debtors_heap = [(-amount, debtor) for debtor, amount in debtors.items()]
        heapq.heapify(debtors_heap)
        while creditors_heap and debtors_heap:
            creditor_amount, creditor = heapq.heappop(creditors_heap)
            debtor_amount, debtor = heapq.heappop(debtors_heap)
            creditor_amount = -creditor_amount
            debtor_amount = -debtor_amount
            transfer_value = min(creditor_amount, debtor_amount)
            transfers.append((creditor, debtor, int(transfer_value)))
            creditor_amount -= transfer_value
            debtor_amount -= transfer_value
            if creditor_amount > 0:
                heapq.heappush(creditors_heap, (-creditor_amount, creditor))
            if debtor_amount > 0:
                heapq.heappush(debtors_heap, (-debtor_amount, debtor))

    return transfers


def read_transfer_data():
    if len(sys.argv) < 2:
        print("No source path given.")
        return None
    else:
        file_path = sys.argv[1]
        if not os.path.exists(file_path):
            print("File", file_path, "not found.")
            return None
        else:
            data = pd.read_csv(file_path, header=None, names=['Creditor', 'Debtor', 'Amount'])
            return data


def save_data(transfers):
    if len(sys.argv) < 3:
        print("No destination path given.")
        return False
    try:
        with open(sys.argv[2], 'w', newline='', encoding='utf-8') as file:
            file_writer = csv.writer(file)
            for transfer in transfers:
                file_writer.writerow(transfer)
        return True
    except Exception as e:
        print(f"Error occurred while saving data: {e}")
        return False


if __name__ == "__main__":
    data = read_transfer_data()
    if data is not None:
        neighbours = load_neighbours_to_graph(data)
        graphs = find_graphs(neighbours)
        balance = data.groupby('Creditor')['Amount'].sum().subtract(data.groupby('Debtor')['Amount'].sum(), fill_value=0)
        debtors_list, creditors_list = split_balances(graphs, balance)
        transfers = calculate_transfers(debtors_list, creditors_list)
        save_data(transfers)
