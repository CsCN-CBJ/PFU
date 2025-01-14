import os
import re
from dataclasses import dataclass
import shutil

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

TEST_DIR: str
ALL_RATIOS = [10, 20, 40, 50, 60, 80, 100, 200]

def renameAndReplace(old, new):
    if os.path.exists(new):
        os.remove(new)
    shutil.move(old, new)

def matchUniqueInt(reStr: str, string: str):
    """
    返回匹配字符串的唯一一个组
    """
    pattern = re.compile(reStr)
    match = pattern.findall(string)
    if len(match) == 0:
        raise KeyError("match failed")
    if len(match) > 1:
        raise KeyError("more than one match", reStr)
    return int(match[0])

@dataclass
class Arguments:
    upgrade_phase: int
    cache_memory: int
    # cache_external: int
    cdc_max_size: int
    cdc_exp_size: int
    cdc_min_size: int
    index_type: str
    external_type: str
    direct_reads: int
    simulation: str
    fake_containers: int

    def getConfigStr(self) -> str:
        return f"upgrade-phase {self.upgrade_phase}, " + \
                f"fingerprint-index-cache-size {self.cache_memory}, " + \
                f"fingerprint-external-cache-size 0, " + \
                f"recipe-cdc-max-size {self.cdc_max_size}, " + \
                f"recipe-cdc-exp-size {self.cdc_exp_size}, " + \
                f"recipe-cdc-min-size {self.cdc_min_size}, " + \
                f"fingerprint-index-key-value {self.index_type}, " + \
                f"upgrade-external-store {self.external_type}, " + \
                f"direct-reads {self.direct_reads}, " + \
                f"simulation-level {self.simulation}, " + \
                f"fake-containers {self.fake_containers}"

def runCommand(level: int, args: Arguments, logName: str):
    ret = os.system(f"bash runEvaluation.sh {level} {TEST_DIR} \"{args.getConfigStr()}\"")
    assert ret == 0
    renameAndReplace(f"{TEST_DIR}/log/{level}.log", f"./data/{logName}")

def matchUniqueSize(file: str) -> int:
    print(f"file: {file}")
    with open(file, "r") as f:
        content = f.read()
    return matchUniqueInt(r"stored data size\(B\): (\d+)", content)

def getCacheSize(uniqueSize: int, ratio: int) -> int:
    if ratio <= 1:
        raise ValueError("ratio should be greater than 1")
    return int(uniqueSize * 0.0025 * ratio / 100)

def getDefaultArgs():
    return Arguments(upgrade_phase=1,
                     cache_memory=10000,
                     cdc_max_size=400,
                     cdc_exp_size=200,
                     cdc_min_size=100,
                     index_type="file",
                     external_type="rocksdb",
                     direct_reads=0,
                     simulation="no",
                     fake_containers=0,
                    )

def runEvaluation():
    global TEST_DIR
    TEST_DIR = "/data/cbj/testing"
    os.system("bash resetEvaluation.sh")
    unique = matchUniqueSize(f"{TEST_DIR}/working/0.log")
    direct = 0

    for level in [1, 2, 3]:
        args = getDefaultArgs()
        args.direct_reads = direct
        args.simulation = "no"
        # os.system(f"rm -rf {TEST_DIR}/working/rocksdb0")
        # os.system(f"rm -rf {TEST_DIR}/working/upgrade_external_cache")
        # runCommand(level, args, f"{level}_container.log")
        args.upgrade_phase = 0
        for ratio in ALL_RATIOS:
            os.system(f"rm -rf {TEST_DIR}/working/rocksdb0")
            os.system(f"rm -rf {TEST_DIR}/working/upgrade_external_cache")
            cacheSize = getCacheSize(unique, ratio)
            args.cache_memory = cacheSize
            cacheSize /= 60 * 900
            args.cdc_max_size = int(cacheSize)
            args.cdc_exp_size = int(cacheSize * 0.75)
            args.cdc_min_size = int(cacheSize * 0.5)
            runCommand(level, args, f"{level}_{direct}_{ratio}.log")

runEvaluation()
