import io
import os
import re
import zipfile
import pathlib

from slurmqueen.slurm_script import base_script
from slurmqueen.experiment import Experiment, ExperimentInstance


class ExperimentConfig:
    """
    A class to store configuration information. Note that the provided directories will be created
    when a job is started if they do not currently exist.

    :param server: the SLURM server.
    :param partition: the partition to use on the SLURM server.
    :param local_directory: a directory on the local machine to use for experimental results
    :param remote_directory: a directory on the SLURM machine to use for experimental computations
                             (typically in a scratch filesystem, if possible).
    """

    def __init__(self, server, partition, local_directory, remote_directory):
        self.__server = server
        self.__partition = partition
        self.__local_directory = local_directory
        self.__remote_directory = remote_directory

    @property
    def server(self):
        return self.__server

    @property
    def partition(self):
        return self.__partition

    @property
    def local_directory(self):
        return self.__local_directory

    @property
    def remote_directory(self):
        return self.__remote_directory


class SlurmExperiment(Experiment):
    def __init__(
        self,
        exp_id,
        *commands,
        dependencies=None,
        setup_commands=None,
        output_argument=">>",
        log_argument="2>",
    ):
        """
        Initialize an experiment.

        :param exp_id: string name of the experiment instance (e.g. 'alpha1')
        :param commands: the Commands to run in each task, relative to experiment folder (e.g. 'python cnfxor.py')
        :param dependencies: a list of files/globs required to run the tool, relative to experiment folder (e.g. 'cnfxor.py')
        :param setup_commands: The setup to perform on each worker node before beginning tasks
        :param output_argument: the argument used to pass the name of the .out file (defaults to stdout)
        :param log_argument: the argument used to pass the name of the .log file (defaults to stderr)
        """
        super().__init__(
            *commands, output_argument=output_argument, log_argument=log_argument
        )

        self.id = exp_id
        if dependencies is None:
            dependencies = []
        self.dependencies = dependencies
        self.setup_commands = setup_commands

    def slurm_instance(self, config):
        """
        Construct an instance of this experiment using the provided configuration.

        If the configuration directories (local and remote) do not exist, they will be created during experiment setup.

        :param config: The runtime server configuration
        :return: An experiment instance, which can generate scripts to run this experiment, can run the scripts on the
                 configured SLURM server, and can run SQL queries on the results.
        """
        return SlurmInstance(self, config)

    def partition_tasks(self, max_size=498):
        """
        Divide the tasks of this experiment into many experiments. This can be used to work around the max job size on
        SLURM servers.

        :param max_size: The maximum number of tasks to include on each experiment.
        :return: A list of experiments, containing in total the same tasks of this experiment.
        """
        num_partitions = int((max_size - 1 + len(self.commands)) / max_size)
        size = int((num_partitions - 1 + len(self.commands)) / num_partitions)

        res = []
        for i in range(num_partitions):
            args_subset = self.commands[(i * size) : (i * size + size)]
            res.append(
                SlurmExperiment(
                    self.id + "/" + str(i),
                    args_subset,
                    dependencies=self.dependencies,
                    setup_commands=self.setup_commands,
                    output_argument=self.output_argument,
                    log_argument=self.log_argument,
                )
            )
        return res

    def prepare_server(self, instance):
        """
        This method will be run after all files are remotely copied but before the task is started.
        """
        pass

    def analyze(self, instance):
        """
        Generate analysis (graphs, etc.) of the experiment.
        """
        pass

    def __str__(self):
        return self.id.replace("\\", "/").split("/")[-1]


class SlurmInstance(ExperimentInstance):
    def __init__(self, experiment_base, config):
        """
        Construct an instance of the provided experiment using the provided configuration.
        This class can generate scripts to run the provided experiment, can run the scripts on the configured SLURM
        server, and can run SQL queries on the results.

        If the configuration directories (local and remote) do not exist, they will be created during experiment setup.

        :param experiment_base: The experiment to run
        :param config: The runtime server configuration
        """
        self._exp = experiment_base
        self._config = config
        ExperimentInstance.__init__(
            self, experiment_base, self.local_project_path(self._exp.id)
        )

    @property
    def server(self):
        return self._config.server

    def job(self):
        """
        Get the most recent job associated with this experiment.

        :return: The most recent job associated with this experiment, or None if none exists.
        """
        jobs = self.jobs()
        if len(jobs) == 0:
            return None

        jobs.sort(key=lambda j: int(j.jobid))
        return jobs[-1]

    def jobs(self):
        """
        Get all jobs associated with this experiment.

        :return: The all jobs associated with this experiment, or None if none exists.
        """
        jobs = self._config.server.all_jobs()
        return list(filter(lambda j: j.name == str(self), jobs))

    def local_project_path(self, filename=""):
        """
        Get the full name for a file contained in the project directory on the local machine.

        :param filename: The name of the file relative to the project directory.
        :return: The full local name of the provided file.
        """
        return self._config.local_directory + "/" + filename

    def remote_experiment_path(self, filename=""):
        """
        Get the full name for a file contained in the experiment directory on the remote machine.

        :param filename: The name of the file relative to the experiment directory.
        :return: The full remote name of the provided file.
        """
        return self._config.remote_directory + "/" + self._exp.id + "/" + filename

    def run(self, num_workers, time, **kwargs):
        """
        Run this experiment on the provided SLURM server. If successful, print the job id used.

        :param num_workers: The number of workers to use for this experiment.
        :param time: The timeout to use for this experiment.
        :param kwargs: Passed to the setup_all function.
        :return: None
        """
        command = self._setup_all(num_workers, time, **kwargs)
        print("Attempting to submit job")
        print(self._config.server.execute(command, timeout=1000))

    def complete(self, force=False):
        """
        Download the results of this experiment back to the local machine. Raises a RuntimeError
        unless all SLURM jobs started by the experiment have completed (set [force] to True to
        override this check).

        :param force: If true, download all current remote data regardless of job status.
        :return: None
        """
        self._gather(force=force)
        self._cleanup(force=force)

    def finished(self, verbose=False):
        """
        Check if the experiment is complete by checking for the existence of output files.

        :param verbose: If true, print a message when not all output files were generated.
        :return: True if the experiment is complete, false otherwise.
        """
        if not os.path.exists(self.local_experiment_path()):
            return False
        output_files = self.output_filenames()
        if len(output_files) > 0:
            if len(output_files) < len(self._exp.commands) and verbose:
                print(
                    "Finished. Only %d/%d output files."
                    % (len(output_files), len(self._exp.commands))
                )
            return True
        return False

    def analyze_or_gui(self, num_workers, time, **kwargs):
        if self.finished():
            return self._exp.analyze(self)
        else:
            return self.ipython_gui(num_workers, time, **kwargs)

    def ipython_gui(self, num_workers, time, **kwargs):
        """
        Build a iPython GUI for running and completing this experiment. All arguments are passed unchanged to "run".

        Requires the ipywidgets package.

        :param num_workers: The number of workers to use for this experiment.
        :param time: The timeout to use for this experiment.
        :param kwargs: Passed to the _setup_all function.
        :return: A GUI to manipulate this job.
        """

        import ipywidgets.widgets

        run_button = ipywidgets.widgets.Button(description="Run", button_style="Danger")
        complete_button = ipywidgets.widgets.Button(
            description="Complete", button_style="info"
        )
        refresh_button = ipywidgets.widgets.Button(description="Refresh", icon="check")
        status_label = ipywidgets.widgets.Label()

        def update(online):
            if not os.path.exists(self.local_experiment_path()):
                status_label.value = "Not started."
                run_button.disabled = False
                complete_button.disabled = True
                return

            output_files = self.output_filenames()
            if len(output_files) == len(self._exp.commands):
                status_label.value = "Finished."
                run_button.disabled = True
                complete_button.disabled = True
                return
            elif len(output_files) > 0:
                status_label.value = "Finished. Only %d/%d output files" % (
                    len(output_files),
                    len(self._exp.commands),
                )
                run_button.disabled = True
                complete_button.disabled = True
                return

            if not online:
                status_label.value = "Unknown"
                run_button.disabled = False
                complete_button.disabled = False
                return

            job = self.job()
            if job is None or job.status() == 0:
                status_label.value = "Unable to find job"
                run_button.disabled = False
                complete_button.disabled = False
                return

            status = self.job().status()
            if set(status.keys()).issubset(
                {"COMPLETED", "TIMEOUT", "CANCELLED+", "CANCELLED"}
            ):
                status_label.value = "Completed running. " + str(status)
                run_button.disabled = True
                complete_button.disabled = False
                return
            else:
                status_label.value = str(status)
                run_button.disabled = True
                complete_button.disabled = True
                return

        update(False)

        run_button.on_click(
            lambda b: self.run(num_workers, time, **kwargs) or update(True)
        )
        complete_button.on_click(lambda b: self.complete() or update(True))
        refresh_button.on_click(lambda b: update(True))

        return ipywidgets.widgets.HBox(
            [run_button, complete_button, refresh_button, status_label]
        )

    def _gather(self, force=False):
        """
        Download the results of the job from the server to the local machine. Raises a RuntimeError
        if the job is currently running, unless [force] is True.

        :param force: If true, download all current remote data regardless of job status.

        :return: None
        """
        # Ensure this experiment has finished
        if not force:
            last = self.job()
            if last and not last.finished(cache=True):
                raise RuntimeError(
                    "Experiment is currently running under JobID " + str(last.jobid)
                )

        with self._config.server.ftp_connect() as ftp:

            def gather_files(zip_name, pattern, display_name):
                # Compress the zip file remotely
                self._config.server.execute(
                    'zip -j %s $(ls %s | grep -E "%s")'
                    % (
                        self.remote_experiment_path(zip_name),
                        self.remote_experiment_path("*"),
                        pattern,
                    ),
                    timeout=1000,
                )

                try:
                    # Copy the zip file
                    ftp.get(
                        self.remote_experiment_path(zip_name),
                        self.local_experiment_path(zip_name),
                    )

                    # Decompress the zip file locally
                    with zipfile.ZipFile(
                        self.local_experiment_path(zip_name)
                    ) as zip_file:
                        zip_file.extractall(path=self.local_experiment_path())
                except FileNotFoundError:
                    print("Unable to find %s files" % display_name)

            print("Experiment complete. Compressing and copying results.")
            gather_files("_outputs.zip", ".*/[0-9]+\.(out|log)", ".out")
            gather_files("_worker_logs.zip", ".*\.worker", ".worker")

    def _cleanup(self, force=False):
        """
        Delete all information for this experiment on the cluster. Raises a RuntimeError if
        the job is currently running, unless [force] is True.

        :param force: If true, delete all current remote data regardless of job status.

        :return: None
        """
        # Ensure this experiment has finished
        if not force:
            last = self.job()
            if last and not last.finished(cache=True):
                raise RuntimeError(
                    "Experiment is currently running under JobID " + str(last.jobid)
                )

        # Delete all files from experiment server
        print("Deleting files from remote server:", self.remote_experiment_path())
        res = self._config.server.execute(
            "rm -r " + self.remote_experiment_path(), timeout=1000
        )
        if res != "":
            print(res)

    def copy_project_files_to_remote(self, files):
        """
        Copy the provided project files to the server, relative to the experiment directory.

        :param files: A list of project files to copy (relative to the project directory).
        :return: None
        """
        with self._config.server.ftp_connect() as ftp:

            project_path = pathlib.Path(self.local_project_path())
            for pattern in files:
                for path in project_path.glob(pattern):
                    if not path.is_file():
                        continue

                    if not path.parent == project_path:
                        # If the dependency belongs in a separate folder, make it
                        self._config.server.execute(
                            "mkdir -p "
                            + self.remote_experiment_path(
                                path.parent.relative_to(project_path).as_posix()
                            )
                        )

                    ftp.put(
                        str(path),
                        self.remote_experiment_path(
                            path.relative_to(project_path).as_posix()
                        ),
                    )

    def _setup_all(self, num_workers, time, cpus_per_worker=1):
        """
        Copy all experiment files from the local machine to the remote server. Prepare scripts to initiate the
        experiment.

        :param num_workers: The number of workers to use for each experiment (maximum 498).
        :param time: The timeout to use for each worker.
        :param cpus_per_worker: The number of CPUs to provide for each worker.
        :return: A command to be run on the SLURM server to run the experiment.
        """

        # Ensure there are no current versions of this experiment running
        last = self.job()
        if last and not last.finished(cache=True):
            raise RuntimeError(
                "Experiment is currently running under JobID " + str(last.jobid)
            )

        # Check if file data already exists on the server for this job
        if self._config.server.execute("ls " + self.remote_experiment_path()) != "":
            res = input(
                "Delete old data files (" + self.remote_experiment_path() + ") [Y/N]: "
            )
            if res.upper() == "Y":
                self._cleanup()
            else:
                raise RuntimeError(
                    "Remove files in "
                    + self.remote_experiment_path()
                    + " or change the experiment id"
                )

        # Compute the number of workers to use, if unspecified
        if num_workers < 0:
            num_workers = len(self)
            print("Running across " + str(num_workers) + " nodes")

        # Create local files
        self.setup()
        input_files = [
            f
            for f in os.listdir(self.local_experiment_path())
            if re.match(r"\d+.in", f)
        ]
        print("Created " + str(len(input_files)) + " local files")

        # Create the remote directory structure
        self._config.server.execute("mkdir -p " + self.remote_experiment_path())

        # Craft the job script for the experiment
        script_builder = base_script()
        script_builder.set("FULL_NAME", str(self))
        script_builder.set("TIME", time)
        script_builder.set("PARTITION", self._config.partition)
        script_builder.set("CPUS", str(cpus_per_worker))
        script_builder.set("NUM_WORKERS", str(num_workers))
        script_builder.set("SETUP", self._exp.setup_commands)

        # Save the job script locally for the experiment
        job_file = "_run.sh"
        with io.open(self.local_experiment_path(job_file), "w", newline="\n") as f:
            f.write(script_builder.build())

        task_file = "_tasks.txt"
        with io.open(self.local_experiment_path(task_file), "w", newline="\n") as f:
            for input_file in input_files:
                f.write(input_file + "\n")

        # Compress input files locally
        input_zip = "_inputs.zip"
        with zipfile.ZipFile(self.local_experiment_path(input_zip), "w") as zipf:
            for input_file in input_files:
                zipf.write(self.local_experiment_path(input_file), input_file)
        print("Compressed local files")

        # Copy input files and script to the remote server
        with self._config.server.ftp_connect() as ftp:
            ftp.put(
                self.local_experiment_path(input_zip),
                self.remote_experiment_path(input_zip),
            )
            ftp.put(
                self.local_experiment_path(job_file),
                self.remote_experiment_path(job_file),
            )
            ftp.put(
                self.local_experiment_path(task_file),
                self.remote_experiment_path(task_file),
            )
        self.copy_project_files_to_remote(self._exp.dependencies)
        print("Copied files to remote server")

        # Decompress input files on remote server
        self._config.server.execute(
            "unzip -q "
            + self.remote_experiment_path(input_zip)
            + " -d "
            + self.remote_experiment_path(),
            timeout=1000,
        )

        # Prepare the job script (and input files) for execution
        self._config.server.execute("chmod +x " + self.remote_experiment_path("*.in"))

        self._exp.prepare_server(self)

        # Generate a command to complete submission
        return 'sbatch --output="{0}" -D "{1}" --array=0-{2} {3} {4}'.format(
            self.remote_experiment_path("slurm_%a.worker"),
            self.remote_experiment_path(""),
            str(num_workers - 1),
            self.remote_experiment_path(job_file),
            str(num_workers),
        )

    def __str__(self):
        return self._exp.__str__()
