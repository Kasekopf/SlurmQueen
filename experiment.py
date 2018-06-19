import io
import os
import json
import zipfile
from slurm_script import base_script, continuation_script
from ipywidgets import widgets

LOCAL_DIRECTORY_BASE = 'C:/Work/Projects/'
REMOTE_DIRECTORY_BASE = 'experiments/'
REMOTE_EXPERIMENT_BASE = '/scratch/jmd11/experiments/'


def copy_dependencies(exp, server, dependencies):
    """
    Copy the provided project files to the server.

    :param exp: The current experiment.
    :param server: A connection to the SLURM server.
    :param dependencies: A list of project files to copy (relative to the project directory).
    :return: None
    """
    with server.ftp_connect() as ftp:
        for filename in dependencies:
            # If the dependency belongs in a separate folder, make it
            if '/' in filename:
                server.execute('mkdir -p ' + exp.remote_project_path(filename[:filename.rfind('/')]))

            ftp.put(exp.local_project_path(filename), exp.remote_project_path(filename))


class Experiment:
    def __init__(self, basename, exp_id, script, time, args,
                 modules=None, changing_args=None):
        """
        Initialize an experiment.

        :param basename: string name of the experiment (e.g. 'CNFXOR')
        :param exp_id: string name of the experiment instance (e.g. 'alpha1')
        :param script: name of the python script to run, relative to experiment folder (e.g. 'cnfxor.py')
        :param time: time to be given to each worker (e.g. '10:00')
        :param args: dictionary to be passed to every task
        :param changing_args: a list of dictionaries; each dictionary defines a new task
        """

        self.basename = basename
        self.id = exp_id
        self.script = script
        self.time = time
        self.args = args
        self.changing_args = changing_args
        self.modules = modules
        if self.modules is None:
            self.modules = []

        self.args['experiment_name'] = basename
        self.args['experiment_id'] = exp_id
        self.args['script'] = script

    def job(self, client):
        """
        Get the most recent job associated with this experiment.

        :param client: A connection to the SLURM server.
        :return: The most recent job associated with this experiment, or None if none exists.
        """
        jobs = self.jobs(client)
        if len(jobs) == 0:
            return None

        jobs.sort(key=lambda j: int(j.jobid))
        return jobs[-1]

    def jobs(self, client):
        """
        Get all jobs associated with this experiment.

        :param client: A connection to the SLURM server.
        :return: The all jobs associated with this experiment, or None if none exists.
        """
        jobs = client.all_jobs()
        return list(filter(lambda j: j.name == self.basename.lower() + '_' + self.id, jobs))

    def local_project_path(self, filename=''):
        """
        Get the full name for a file contained in the project directory on the local machine.

        :param filename: The name of the file relative to the project directory.
        :return: The full local name of the provided file.
        """
        return LOCAL_DIRECTORY_BASE + self.basename + '/' + filename

    def remote_project_path(self, filename=''):
        """
        Get the full name for a file contained in the project directory on the remote machine.

        :param filename: The name of the file relative to the project directory.
        :return: The full remote name of the provided file.
        """
        return self.remote_experiment_path(filename)
        # REMOTE_DIRECTORY_BASE + self.basename.lower() + '/' + filename

    def local_experiment_path(self, filename=''):
        """
        Get the full name for a file contained in the experiment directory on the local machine.

        :param filename: The name of the file relative to the experiment directory.
        :return: The full local name of the provided file.
        """
        return self.local_project_path('experiments/' + self.id + '/' + filename)

    def remote_experiment_path(self, filename=''):
        """
        Get the full name for a file contained in the experiment directory on the remote machine.

        :param filename: The name of the file relative to the experiment directory.
        :return: The full remote name of the provided file.
        """
        return REMOTE_EXPERIMENT_BASE + self.basename.lower() + '/' + self.id + '/' + filename

    def partition(self, max_size=498):
        """
        Divide the tasks of this experiment into many experiments. This can be used to work around the max job size on
        SLURM servers.

        :param max_size: The maximum number of tasks to include on each experiment.
        :return: A list of experiments, containing in total the same tasks of this experiment.
        """
        num_partitions = int((max_size - 1 + len(self.changing_args)) / max_size)
        size = int((num_partitions - 1 + len(self.changing_args)) / num_partitions)

        res = []
        for i in range(num_partitions):
            args_subset = self.changing_args[(i*size):(i*size+size)]
            res.append(Experiment(self.basename, self.id + '/' + str(i), self.script, self.time,
                                  self.args, args_subset))
        return res

    def run(self, client, num_workers, **kwargs):
        """
        Run this experiment on the provided SLURM server. If successful, print the job id used.

        :param client: A connection to the SLURM server.
        :param num_workers: The number of workers to use for this experiment.
        :param kwargs: Passed to the _setup function.
        :return: None
        """
        if num_workers < 0:
            num_workers = len(self)
            print('Running across ' + str(num_workers) + ' nodes')

        command = self._setup(client, num_workers, **kwargs)
        print('Attempting to submit job')
        print(client.execute(command))

    def complete(self, client):
        """
        Download the results of this experiment back to the local machine. This can only run successfully when all
        SLURM jobs started by the experiment have completed.

        :param client: A connection to the SLURM server.
        :return: None
        """
        self._gather(client)
        self._cleanup(client)
        self._postprocess()

    def finished(self, verbose=False):
        """
        Check if the experiment is complete by checking for the existence of output files.

        :param verbose: If true, print a message when not all output files were generated.
        :return: True if the experiment is complete, false otherwise.
        """
        output_files = self.output_filenames()
        if len(output_files) > 0:
            if len(output_files) < len(self.changing_args) and verbose:
                print('Finished. Only %d/%d output files.' % (len(output_files), len(self.changing_args)))
            return True
        return False

    def ipython_gui(self, client, num_workers, **kwargs):
        """
        Build a iPython GUI for running and completing this experiment. All arguments are passed unchanged to "run".

        :param client: A connection to the SLURM server.
        :param num_workers: The number of workers to use for this experiment.
        :param kwargs: Passed to the _setup function.
        :return: A GUI to manipulate this job.
        """
        run_button = widgets.Button(description='Run', button_style='Danger')
        complete_button = widgets.Button(description='Complete', button_style='info')
        refresh_button = widgets.Button(description='Refresh', icon='check')
        status_label = widgets.Label()

        def update(online):
            if not os.path.exists(self.local_experiment_path()):
                status_label.value = 'Not started.'
                run_button.disabled = False
                complete_button.disabled = True
                return

            output_files = self.output_filenames()
            if len(output_files) == len(self.changing_args):
                status_label.value = 'Finished.'
                run_button.disabled = True
                complete_button.disabled = True
                return
            elif len(output_files) > 0:
                status_label.value = 'Finished. Only %d/%d output files' % (len(output_files), len(self.changing_args))
                run_button.disabled = True
                complete_button.disabled = True
                return

            if not online:
                status_label.value = 'Unknown'
                run_button.disabled = False
                complete_button.disabled = False
                return

            job = self.job(client)
            if job is None or job.status() == 0:
                status_label.value = 'Unable to find job'
                run_button.disabled = False
                complete_button.disabled = False
                return

            status = self.job(client).status()
            if set(status.keys()).issubset({'COMPLETED', 'TIMEOUT', 'CANCELLED+', 'CANCELLED'}):
                status_label.value = 'Completed running. ' + str(status)
                run_button.disabled = True
                complete_button.disabled = False
                return
            else:
                status_label.value = str(status)
                run_button.disabled = True
                complete_button.disabled = True
                return
        update(False)

        run_button.on_click(lambda b: self.run(client, num_workers, **kwargs) or update(True))
        complete_button.on_click(lambda b: self.complete(client) or update(True))
        refresh_button.on_click(lambda b: update(True))

        return widgets.HBox([run_button, complete_button, refresh_button, status_label])

    def _preprocess(self, client):
        """
        This method will be run after all files are remotely copied but before the task is started.
        """
        pass

    def _postprocess(self):
        """
        This method will be run after all files have been transferred from the server.
        """
        pass

    def analyze(self):
        """
        Generate analysis (graphs, etc.) of the experiment.
        """
        pass

    def _gather(self, client):
        """
        Download the results of the job from the server to the local machine.

        :param client: A connection to the SLURM server.
        :return: None
        """
        # Ensure this experiment has finished
        last = self.job(client)
        if last and not last.finished(cache=True):
            raise RuntimeError('Experiment is currently running under JobID ' + str(last.jobid))

        # Compress all *.out files into a single remote .zip
        print('Experiment complete. Compressing results.')
        output = '_outputs.zip'
        client.execute('zip -j ' + self.remote_experiment_path(output) + ' ' + self.remote_experiment_path('*.out'))

        # Copy the output zip file locally
        print('Copying results to local directory')
        with client.ftp_connect() as ftp:
            ftp.get(self.remote_experiment_path(output), self.local_experiment_path(output))

        # Decompress output files locally
        print('Decompressing local results')
        with zipfile.ZipFile(self.local_experiment_path(output)) as zip_file:
            zip_file.extractall(path=self.local_experiment_path())

    def _cleanup(self, client):
        """
        Delete all information for this experiment on the cluster.

        :param client: A connection to the SLURM server.
        :return: None
        """
        # Ensure this experiment has finished
        last = self.job(client)
        if last and not last.finished(cache=True):
            raise RuntimeError('Experiment is currently running under JobID ' + str(last.jobid))

        # Delete all files from experiment server
        print('Deleting files from remote server:', self.remote_experiment_path(''))
        res = client.execute('rm -r ' + self.remote_experiment_path(''))
        if res != '':
            print(res)

    def _setup(self, client, num_workers, cpus_per_worker=1, partition='commons'):
        """
        Copy all experiment files from the local machine to the remote server. Prepare scripts to initiate the
        experiment.

        :param client: A connection to the SLURM server.
        :param num_workers: The number of workers to use for each experiment (maximum 498).
        :param cpus_per_worker: The number of CPUs to provide for each worker.
        :param partition: The SLURM server partition to use.
        :return: A command to be run on the SLURM server to run the experiment.
        """

        # Ensure there are no current versions of this experiment running
        last = self.job(client)
        if last and not last.finished(cache=True):
            raise RuntimeError('Experiment is currently running under JobID ' + str(last.jobid))

        # Check if file data already exists on the server for this job
        if client.execute('ls ' + self.remote_experiment_path()) != '':
            res = input('Delete old data files (' + self.remote_experiment_path() + ') [Y/N]: ')
            if res.upper() == 'Y':
                self._cleanup(client)
            else:
                raise RuntimeError('Remove files in ' + self.remote_experiment_path() + ' or change the experiment id')

        # Create the local directory structure
        if not os.path.exists(self.local_experiment_path()):
            os.makedirs(self.local_experiment_path())

        # Create the remote directory structure
        client.execute('mkdir -p ' + self.remote_experiment_path())
        client.execute('mkdir -p ' + self.remote_project_path())
        self.args["project_directory"] = self.remote_project_path()

        # Provide workers with a temporary directory to use
        client.execute('mkdir -p ' + self.remote_experiment_path('temp/'))
        self.args["temp_directory"] = self.remote_experiment_path("temp/")

        # Craft the job script for the experiment
        script_builder = base_script()

        script_builder.set("ARGS", '"' + json.dumps(self.args).replace('"', '\\"') + '"')
        script_builder.set("SCRIPT", self.remote_project_path(self.script))
        script_builder.set("PROJECT", self.remote_project_path(""))
        script_builder.set("FULL_NAME", self.basename.lower() + '_' + self.id)
        script_builder.set("ID", str(self.id))
        script_builder.set("IN", '"' + self.remote_experiment_path('$SLURM_ARRAY_TASK_ID.in') + '"')
        script_builder.set("TIME", str(self.time))
        script_builder.set("PARTITION", partition)
        script_builder.set("CPUS", str(cpus_per_worker))

        modules_load = ''
        for m in self.modules:
            modules_load += 'module load ' + m + '\n'
        script_builder.set("MODULES", modules_load)

        # Save the job script locally for the experiment
        job_file = '_run.sh'
        with io.open(self.local_experiment_path(job_file), 'w', newline='\n') as f:
            f.write(script_builder.build())

        # Craft input files for each worker
        input_files = [str(worker_id) + '.in' for worker_id in range(num_workers)]
        inputs = [io.open(self.local_experiment_path(input_file), 'w', newline='\n') for input_file in input_files]
        for count, fresh_args in enumerate(self.changing_args):
            fresh_args = dict(fresh_args)

            number = str(count).zfill(len(str(len(self.changing_args))))  # Prepend with padded zeros
            fresh_args['output_file'] = self.remote_experiment_path(number + '.out')
            fresh_args['task_id'] = str(count)

            inputs[count % num_workers].write(json.dumps(fresh_args))
            inputs[count % num_workers].write('\n')
        for input_file in inputs:
            input_file.close()

        input_files.append(job_file)  # Include the job script in the list of files to copy
        print('Created local files')

        # Compress input files locally
        input_zip = '_inputs.zip'
        with zipfile.ZipFile(self.local_experiment_path(input_zip), 'w') as zipf:
            for input_file in input_files:
                zipf.write(self.local_experiment_path(input_file), input_file)
        print('Compressed local files')

        # Copy input files and script to the remote server
        with client.ftp_connect() as ftp:
            ftp.put(self.local_experiment_path(input_zip), self.remote_experiment_path(input_zip))

            if '/' in self.script:
                client.execute('mkdir -p ' + self.remote_project_path(self.script[:self.script.rfind('/')]))
            ftp.put(self.local_project_path(self.script), self.remote_project_path(self.script))

        print('Copied files to remote server')

        # Decompress input files on remote server
        client.execute('unzip ' + self.remote_experiment_path(input_zip) + ' -d ' + self.remote_experiment_path())

        # Prepare the job script for execution
        client.execute('chmod +x ' + self.remote_experiment_path(job_file))

        self._preprocess(client)

        # Generate a command to complete submission
        return 'sbatch --output={0} --array=0-{1} {2} {3}'.format(self.remote_experiment_path('slurm-%A_%a.out'),
                                                                  str(num_workers - 1),
                                                                  self.remote_experiment_path(job_file),
                                                                  str(num_workers))

    def output_filenames(self):
        """
        Get all files that were output by this experiment.

        :return: A list of full paths, each one an output file of this experiment.
        """
        result = []
        for filename in os.listdir(self.local_experiment_path()):
            if 'slurm' in filename:
                continue
            if not filename.endswith('.out'):
                continue
            result.append(self.local_experiment_path(filename))
        return result

    def __len__(self):
        return len(self.changing_args)

    def __str__(self):
        return "<" + self.basename + ":" + self.id + ">"


def run_chain(experiments, client, num_workers=498, partition='commons', **kwargs):
    """
    Run the set of experiment as a chain. That is, each experiment will run in order, with each experiment beginning
    when all jobs started by the previous experiment have completed.

    :param experiments: A list of experiments to run.
    :param client: A connection to the SLURM server.
    :param num_workers: The number of workers to use for each experiment (maximum 498).
    :param partition: The SLURM server partition to use.
    :param kwargs: Passed to the _setup function.
    :return: None
    """
    if num_workers > 498:
        raise RuntimeError('At most 498 workers can be used')
    if len(experiments) == 0:
        raise RuntimeError('No experiments provided')
    if len(experiments) == 1:
        print('Only one experiment specified; chain is not needed')
        experiments[0].run(client, num_workers, partition=partition, **kwargs)
        return

    chain_file = '_start.sh'

    for exp, next_exp in zip(experiments, experiments[1:] + [None]):
        print('Initializing', exp)
        start = exp.setup(client, num_workers, partition=partition, **kwargs)

        # Craft the continuation script to begin the next round
        script_builder = continuation_script()
        script_builder.set("START_JOB", start)
        script_builder.set("PARTITION", partition)
        script_builder.set("FULL_NAME", exp.basename.lower() + '_' + exp.id)

        if next_exp is None:
            script_builder.set("START_NEXT_LINK", "# No further link to run")
        else:
            next_link_command = 'sbatch'
            next_link_command += ' --output=' + next_exp.remote_experiment_path('slurm-starter.out')
            next_link_command += ' --dependency afterany:$jobid'
            if exp != experiments[0]:
                next_link_command += ':$SLURM_JOB_ID'  # Also wait for the current monitor job to finish

                next_link_command += ' ' + next_exp.remote_experiment_path(chain_file)

            script_builder.set("START_NEXT_LINK", next_link_command)

        with io.open(exp.local_experiment_path(chain_file), 'w', newline='\n') as f:
            f.write(script_builder.build())

    print('Copying continuation files to remote server')
    ftp = client.open_sftp()
    for exp in experiments:
        ftp.put(exp.local_experiment_path(chain_file), exp.remote_experiment_path(chain_file))
        # Prepare the chain script for execution
        client.execute('chmod +x ' + exp.remote_experiment_path(chain_file))
    ftp.close()

    print('Starting first link')
    client.execute(experiments[0].remote_experiment_path(chain_file))
