from rucio.client import Client
import uproot
import json
from tqdm import tqdm

RSE = "LRZ-LMU_LOCALGROUPDISK"


def get_replica_info(dids: list[dict], rse: str=RSE) -> dict:
    client = Client()
    response = client.list_replicas(dids, rse_expression=rse)
    # make the result a dict of name -> info instead of a list of infos
    result = {}
    for r in response:
        # make sure name is unique
        assert r["name"] not in result
        result[r["name"]] = r

    return result


def transform_ntuplelist(original: dict, replica_infos: dict, verify_num_entries=False) -> dict:
    """
    change all paths in the original ntuples list with the path obtained
    from replica_infos
    """
    new = {}
    for sample in original:
        print(f"{sample}...")
        new[sample] = {}
        for variation in original[sample]:
            print(f"  {variation}")
            new[sample][variation] = {}
            new[sample][variation]["nevts_total"] = original[sample][variation]["nevts_total"]
            new[sample][variation]["files"] = []
            for file in tqdm(original[sample][variation]["files"]):
                name = file["path"].split("/")[-1]
                if name not in replica_infos:
                    raise KeyError(f"file {name} does not exist as a replica")
                for pfn in replica_infos[name]["pfns"]:
                    if pfn.startswith("root://"):
                        path = pfn
                        break
                else:
                    raise KeyError(f"file {name} has no root:// url to it")
            
                if verify_num_entries:
                    ntuple = uproot.open(path)
                    tree = ntuple["Events"]
                    nevts = tree.num_entries
                    ntuple.close()
                    if nevts != file["nevts"]:
                        raise ValueError(f"number of entries for file {name} do not match with original ntuple list")
                else:
                    nevts = file["nevts"]

                new_file = {
                    "path": path,
                    "nevts": nevts,
                }
                new[sample][variation]["files"].append(new_file)

    return new


if __name__ == "__main__":
    with open("../nanoaod_inputs.json") as f:
        unl_ntuplelist = json.load(f)

    replica_infos = get_replica_info(dids=[{"scope": "user.dakoch", "name": "agc-1.0.0"}])
    lrz_ntuplelist = transform_ntuplelist(unl_ntuplelist, replica_infos)

    with open("../ntuples_lrz.json", "w") as f:
        json.dump(lrz_ntuplelist, f, indent=4)
