class ScriptBuilder:
    def __init__(self, script):
        """
        Initialize a shell script. This class allows for tags, e.g. [$$TAG_NAME], in the unbuilt script to be filled in.

        :param script: The unbuilt script.
        """
        self._script = script
        self._settings = {}

    def set(self, key, value):
        """
        Set the tag "key" in the SLURM script to the value "value".

        :param key: The key to set.
        :param value: The value to set "key" to.
        :return:
        """
        self._settings[key] = value

    def build(self):
        """
        Build the shell script, including the value of all specified tags.

        :return: The built shell script.
        """
        result = self._script
        for key, value in self._settings.items():
            result = result.replace('[$$' + key + ']', value)
        return result


def base_script():
    """
    Get the base script to run a single experiment.

    :return: An unbuilt script to run a single experiment.
    """
    return ScriptBuilder("""#!/bin/bash
#SBATCH --time=[$$TIME]
#SBATCH --job-name=[$$FULL_NAME]
#SBATCH --partition=[$$PARTITION]
#SBATCH --nodes=1
#SBATCH --cpus-per-task=[$$CPUS]
#SBATCH --mem-per-cpu=4000m

[$$SETUP]

cd [$$PROJECT]
for file in `find *.in | awk "(NR - 1) % $1 == $SLURM_ARRAY_TASK_ID"`; do
  ./$file
done
""")

def continuation_script():
    """
    Get the base script to run a sequence of experiments.

    :return: An unbuilt script to run a sequence of experiments.
    """
    return ScriptBuilder("""#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --job-name=[$$FULL_NAME]_starter
#SBATCH --partition=[$$PARTITION]
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=4000m

jobid=$([$$START_JOB])
jobid=${jobid:20}
echo "Submitted primary job: $jobid"
[$$START_NEXT_LINK]
    """)
