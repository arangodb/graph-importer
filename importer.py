import sys
import requests
import os.path


def bulk_insert(endpoint, collection, documents):
    url = endpoint + os.path.join("/_api/document/", collection)
    response = requests.post(url, json=documents)
    if response.status_code != 202:
        raise RuntimeError(f"Invalid response from bulk insert{response.text}")


def file_reader(filename, number_of_lines):
    with open(filename, "r") as f:
        res = list()
        for line in f:
            res.append(line.strip())
            if len(res) == 100000:
                yield res
                res = list()
        if len(res) != 0:
            yield res


def main(endpoint, vertex_file, edge_file):
    # for vids in file_reader(vertex_file, 1000):
    #    documents = ({"_key": vid_} for vid_ in vids)
    #    bulk_insert(endpoint, "v", list(documents))
    #    print("Write vertex")

    for eids in file_reader(edge_file, 1000):
        documents = list()
        for i in eids:
            f, t = i.split(' ', 2)
            w = 0
            documents.append({"_from": f"v/{f}", "_to": f"v/{t}", "weight": w})
        bulk_insert(endpoint, "e", list(documents))
        print("Write edge")


if __name__ == "__main__":
    main(*sys.argv[1:])
