from slurmqueen.ssh_client import SSHServer


class SlurmServer(SSHServer):
    """
    A class for interacting with a Slurm server.
    """

    def __init__(self, server, username, key_file):
        """
        Initialize a connection to a Slurm server. No network connection is actually attempted until needed.

        :param server: The address of the server
        :param username: The username to use on the server
        :param key_file: The rsa .ssh keyfile to use to login to the server
        """
        SSHServer.__init__(self, server, username, key_file)

    def current_jobs(self, job_id=None, other_username=None):
        """
        Load information on all jobs using squeue. Batch jobs of the same batch are grouped together.

        By default, load all jobs belonging to the login username. If arguments are given, load information
        on all jobs specified by a job id or belonging to a specific user.

        :param job_id: If provided, load all jobs whose job id matches this.
        :param other_username: If provided, load all jobs belonging to this user.
        :return: A list of jobs, grouped by batch.
        """
        command = 'squeue --format="%.20i %.9P %j %.8u %.8T %.10M %.9l %.6D %R"'
        if job_id:
            command += " -j " + str(job_id)
        elif other_username:
            command += " -u " + other_username
        else:
            command += " -u " + self.username

        raw_jobs = self.execute(command).split("\n")
        raw_jobs = list(filter(lambda j: j != "", raw_jobs))

        if len(raw_jobs) < 1:
            return []
        res = [JobData(raw_jobs[0], i) for i in raw_jobs[1:]]
        res = filter(lambda j: j.name != "batch", res)

        return BatchJob.collect(self, res)

    def all_jobs(self, job_id=None, other_username=None, command=None):
        """
        Load information on all jobs using sacct. Batch jobs of the same batch are grouped together.

        By default, load all jobs belonging to the login username. If arguments are given, load information
        on all jobs specified by a job id or belonging to a specific user.

        :param job_id: If provided, load all jobs whose job id matches this.
        :param other_username: If provided, load all jobs belonging to this user.
        :param command: If provided, execute it instead of a default sacct command.
        :return: A list of jobs, grouped by batch.
        """
        if not command:
            command = 'sacct --format="jobid%20,jobname%50,partition,user,state,totalcpu,time,node,maxrss" --parsable2'  # parsable2 shows '|'-delimited values, including max resident set size (maxrss)
        if job_id:
            command += " -j " + str(job_id)
        elif other_username:
            command += " -u " + other_username
        else:
            command += " -u " + self.username

        raw_jobs = self.execute(f"{command} --units=K").split(
            "\n"
        )  # maxrss in kilobytes
        raw_jobs = list(filter(lambda j: j != "", raw_jobs))

        if len(raw_jobs) < 2:
            return []

        res = [
            JobData(raw_jobs[0], i, parsable2=True) for i in raw_jobs[1:]
        ]  # sacct --parsable2: header row is immediately followed by data rows

        def accumulateMaxResidentSetSizes():  # modifies `res` (list of JobData objects)
            """
            Add to the maxrss field of each primary JobData object (e.g. "1234567_0") the maxrss fields of relevant secondary JobData objects (e.g. "1234567_0.batch" and "1234567_0.extern") in `res`.

            This accumulation is necessary because "*.batch" or "*.extern" may be filtered out of `res` later.
            """
            i = 0  # index of a primary JobData object in `res`
            while i < len(res):
                if "." in res[i].jobid:
                    print(
                        f'[WARNING] "." in primary JobData.jobid "{res[i].jobid}"',
                        file=sys.stderr,
                    )
                    return
                else:
                    j = i + 1  # index of a relevant secondary JobData object in `res`
                    while j < len(res) and res[j].jobid.startswith(res[i].jobid):
                        res[i].maxrss += res[j].maxrss
                        j += 1
                    i = j  # the next primary JobData object follows the last relevant secondary JobData object of the current primary JobData ojbect

        accumulateMaxResidentSetSizes()

        res = filter(lambda j: j.name != "batch", res)
        return BatchJob.collect(self, res)

    def job(self, job_id):
        """
        Load information on a single job (using sacct), specified by job id.

        :param job_id: Load the most recent job whose job id matches this.
        :return: The most recent job with matching job id, or None if no such jobs exist.
        """
        res = self.all_jobs(job_id=job_id)
        if not res:
            return None
        if len(res) > 1:
            print("Identified " + str(len(res)) + " jobs; returning most recent job")
        return res[-1]


class JobData:
    def __init__(self, header, info, parsable2=False):
        if parsable2:  # "sacct --parsable2" in method SlurmServer.all_jobs
            words = info.split(
                "|"
            )  # there may be an empty value between 2 delimiters, i.e. "||"
            header_words = header.upper().split("|")  # every header word is nonempty
        else:  # "squeue" in method SlurmServer.current_jobs
            words = filter(lambda w: len(w) > 0, info.split(" "))
            header_words = filter(lambda w: len(w) > 0, header.upper().split(" "))

        self.properties = {}
        for column, value in zip(header_words, words):
            if column == "JOBID":
                self.jobid = value
            if column == "PARTITION":
                self.partition = value
            if column == "NAME" or column == "JOBNAME":
                self.name = value
            if column == "USER":
                self.user = value
            if column == "STATE":
                self.state = value
            if column == "TIME" or column == "TOTALCPU":
                self.time = value
            if column == "TIME_LIMIT" or column == "TIMELIMIT":
                self.time_limit = value
            if column == "NODES":
                self.nodes = value
            if column == "NODELIST(REASON)" or column == "NODELIST":
                self.nodelist = value
            if column == "MAXRSS":  # max resident set size (RAM usage)
                self.maxrss = 0
                if value.endswith("K"):
                    self.maxrss = int(value[:-1])
                elif value not in {"", "0"}:
                    print(
                        f'[WARNING] "sacct --units=K" should have showed kilobytes but actually showed {value}',
                        file=sys.stderr,
                    )

            self.properties[column] = value

    def __str__(self):
        return "[" + self.jobid + ":" + self.name + ":" + self.user + "]"


class BatchJob:
    def __init__(self, server, jobs):
        def extract_id(job):
            return job.jobid.split("_")[0]

        if not jobs:
            raise ValueError("No jobs provided to JobBatch")

        rep = jobs[0]
        self.server = server
        self.jobid = extract_id(rep)
        self.name = rep.name
        self.user = rep.user
        self.jobs = jobs

        highest_val = 0
        for j in jobs:
            if "_" in j.jobid and not j.jobid.endswith(".extern"):
                if "-" in j.jobid:
                    # JobID has the form 000000_[0-##]
                    top = j.jobid.split("-")[1][:-1]
                    top = int(top)
                elif "[" in j.jobid:
                    # JobID has the form 000000_[###]
                    top = int(j.jobid.split("[")[1][:-1])
                else:
                    # JobID has the form 000000_###
                    top = int(j.jobid.split("_")[1])
                highest_val = max(highest_val, top)
        self.count = highest_val + 1

        # Ensure the batch of jobs consists of a single job
        for j in jobs:
            if extract_id(j) != self.jobid:
                raise ValueError(
                    "JobBatch created with id "
                    + self.jobid
                    + " but contains "
                    + extract_id(j)
                )

    def refresh(self):
        new_batch = self.server.job(self.jobid)
        self.jobs = new_batch.jobs

    def finished(self, cache=False):
        status = self.status(cache)

        ongoing = [
            "PENDING",
            "CONFIGURING",
            "COMPLETING",
            "PENDING",
            "RUNNING",
            "PREEMPTED",
        ]
        for state in ongoing:
            if state in status:
                return False
        return True

    def cancel(self):
        self.server.execute("scancel " + str(self.jobid))

    def status(self, cache=False):
        if not cache:
            self.refresh()

        res = {}
        for j in self.jobs:
            if "-" in j.jobid:
                # JobID has the form 000000_[##-##]
                bot = j.jobid.split("[")[1].split("-")[0]
                top = j.jobid.split("-")[1][:-1]

                count = (
                    int(top) - int(bot) + 1
                )  # Both boundaries are inclusive on SLURM
            else:
                # JobID represents a single job
                count = 1

            if j.state not in res:
                res[j.state] = 0
            res[j.state] += count
        return res

    def __str__(self):
        return (
            "["
            + self.jobid
            + "_[0-"
            + str(self.count - 1)
            + "]:"
            + self.name
            + ":"
            + self.user
            + "]"
        )

    @staticmethod
    def collect(server, jobs):
        jobs_by_id = {}
        for j in jobs:
            if (
                ".b" in j.jobid or ".e" in j.jobid
            ):  # from ".b+" to ".batch" or from ".e+" to ".extern", depending on the width of the JobID column
                continue

            job_id = j.jobid.split("_")[0]
            if job_id in jobs_by_id:
                jobs_by_id[job_id].append(j)
            else:
                jobs_by_id[job_id] = [j]

        res = [BatchJob(server, j) for j in jobs_by_id.values()]
        res.sort(key=lambda job: int(job.jobid))
        return res
