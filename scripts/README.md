WARNING: All scripts are using the path `~/destor/` as the destor source code directory. If you have the source code in a different directory, you need to change the path in the scripts.

1. run `bash runCorrect.sh <upgrade_method>` to run the correctness test.
2. run `bash backup.sh <src_dir> <working_dir>` to backup the dataset.
3. set configurations in runEvaluation.py and run `python3 runEvaluation.py` to run the evaluation.
