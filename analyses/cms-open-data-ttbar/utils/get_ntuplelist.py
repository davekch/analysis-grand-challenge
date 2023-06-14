from rucio.client import Client
import uproot
import json
from tqdm import tqdm


SCOPE = "user.ivukotic"
dids = [
    "user.ivukotic.ttbar__nominal",
    "user.ivukotic.ttbar__scaledown",
    "user.ivukotic.ttbar__scaleup",
    "user.ivukotic.ttbar__ME_var",
    "user.ivukotic.ttbar__PS_var",
    "user.ivukotic.single_top_s_chan__nominal",
    "user.ivukotic.single_top_t_chan__nominal",
    "user.ivukotic.single_top_tW__nominal",
    "user.ivukotic.wjets__nominal",
]
RSE = "LRZ-LMU_LOCALGROUPDISK"
TREENAME = "events"


def get_replica_info(name, scope=SCOPE, rse=RSE):
    client = Client()
    replicas_info = {
        "files": [],
        "nevts_total": 0
    }
    # get info about all files in this dataset
    replicas_info_raw = client.list_replicas(
        [{"scope": scope, "name": name}], rse_expression=rse
    )
    # this for loop is super slow because there are a ton of items -> do this concurrently?
    for replica in tqdm(replicas_info_raw):
        # if there is more than one url, you misunderstood the output of this thingy
        assert len(replica["rses"][RSE]) == 1, "quack!"
        # get the url
        info = {"path": replica["rses"][RSE][0]}
        # get the number of events in this file
        with uproot.open(info["path"] + ":" + TREENAME) as tree:
            info["nevts"] = tree.num_entries

        replicas_info["files"].append(info)
        replicas_info["nevts_total"] += info["nevts"]

    return replicas_info


ntuples_info = {}

for did in dids:
    dataset = did.split(".")[2]
    sample, variation = dataset.split("__")
    if sample not in ntuples_info:
        ntuples_info[sample] = {}

    print(f"get info for {sample} ({variation})")
    ntuples_info[sample][variation] = get_replica_info(did)

    # save after every loop because some iterations are super slow and it might crash in the meantime
    with open("ntuples_lrz.json", "w") as f:
        json.dump(ntuples_info, f, indent=4)
