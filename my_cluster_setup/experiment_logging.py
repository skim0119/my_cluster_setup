import os

def collect_logfiles(path, tag="log"):
    str_list = []
    for root, dirs, files in os.walk(path):
        for filename in files:
            fpath = os.path.join(root, filename)
            if tag not in filename:
                continue
            if not os.path.exists(fpath):  # TODO: This shouldn't be needed
                continue
            str_list.append(f"log path: {fpath}")
            with open(fpath, "r") as log:
                str_list.extend(log.readlines())
            str_list.append("")
            str_list.append("-"*80)
            str_list.append("")
    return '\n'.join(str_list)
