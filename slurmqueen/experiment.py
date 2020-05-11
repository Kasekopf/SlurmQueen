import ast
import io
import itertools
import os
import pandas as pd
import pandas.io.sql
import sqlite3


class Experiment:
    def __init__(self, *command_configs, output_argument=">>", log_argument="2>"):
        """
        Initialize an experiment, defined by running all Commands in command_configs.

        If multiple positional arguments are included, the resulting experiment is the
        cartesian product of the lists of commands provided for each argument.

        The commands should be stated relative to experiment folder (e.g. 'python cnfxor.py')

        :param command_configs: each positional argument is a list of commands to run.
        :param args: a list of dictionaries; each dictionary defines a new task.
        :param output_argument: the argument name used to pass the name of the .out file (defaults to stdout)
        :param log_argument: the argument name used to pass the name of the .log file (defaults to stderr)
        """

        def fix_legacy(configs):
            # Legacy: strings represent a single command to run
            if isinstance(configs, str):
                return [Command(configs)]

            return [
                # Legacy: a dict can also represent a command
                Command(**c) if isinstance(c, dict) else c
                for c in configs
            ]

        command_configs = [fix_legacy(configs) for configs in command_configs]
        if len(command_configs) == 1:
            self.commands = command_configs[0]
        else:
            self.commands = [
                Command(*combo) for combo in itertools.product(*command_configs)
            ]

        self.output_argument = output_argument
        self.log_argument = log_argument

    def instance(self, local_directory):
        """
        Construct an instance of this experiment located in the provided local_directory.

        If local_directory does not exist, it will be created during experiment setup.

        :param local_directory: A directory to store input and output data for this experiment
        :return: An experiment instance, which can generate scripts to run this experiment and can run SQL queries on
                 the results.
        """
        return ExperimentInstance(self, local_directory)


class ExperimentInstance:
    def __init__(self, experiment_base, local_directory):
        """
        Construct an instance of the provided experiment located in the provided local_directory.
        This class can generate scripts to run the provided experiment and can run SQL queries on the results.

        If local_directory does not exist, it will be created during experiment setup.

        :param experiment_base: The experiment to run
        :param local_directory: The directory to place .in files and read .out files (created if does not exist)
        """
        self._exp = experiment_base
        self._local_directory = local_directory

    def local_experiment_path(self, filename=""):
        """
        Get the full name for a file contained in the local directory.

        :param filename: The name of the file relative to the local directory.
        :return: The full local name of the provided file.
        """
        return self._local_directory + "/" + filename

    def setup(self):
        """
        Create .in files for this experiment in the local directory.

        The local directory is created if it does not exist.

        :return: None
        """
        # Create the local directory structure
        if not os.path.exists(self._local_directory):
            os.makedirs(self._local_directory)

        # Craft input files for each worker
        for count, command in enumerate(self._exp.commands):
            number = str(count).zfill(
                len(str(len(self._exp.commands)))
            )  # Prepend with padded zeros

            input_file = io.open(
                self.local_experiment_path(number + ".in"), "w", newline="\n"
            )

            # Write the arguments as the first line of the output file
            input_file.write(
                'echo "%s" > $(dirname $0)/%s.out'
                % (repr(command.get_table()).replace('"', '\\"'), number)
            )
            input_file.write("\n")

            # Setup the redirection for output and log files.
            output_args = Arg.parse_all_from(
                self._exp.output_argument, "$(dirname $0)/%s.out" % number
            )
            log_args = Arg.parse_all_from(
                self._exp.log_argument, "$(dirname $0)/%s.log" % number
            )
            full_command = Command(command, *output_args, *log_args)
            input_file.write(" ".join(full_command.get_args()))

            input_file.write("\n")
            input_file.close()

    def output_filenames(self):
        """
        Get all files that were output by this experiment.

        :return: A list of full paths, each one an output file of this experiment.
        """
        result = []
        for filename in os.listdir(self.local_experiment_path()):
            if filename.endswith(".out"):
                result.append(self.local_experiment_path(filename))
        return result

    def __len__(self):
        return len(self._exp.commands)

    def query(self, query):
        """
        Query the database of results.

        :param query: An SQL query to run
        :return: A pandas dataframe with the results of the query
        """
        if not os.path.exists(self.local_experiment_path("_results.db")):
            self.create_database()

        with self.get_database() as db:
            data = pd.read_sql_query(query, db)
            return data

    def get_database(self):
        """
        Open a connection to the database used to cache results.

        After running create_database, the "data" table will contain the data generated as a result of each task.

        :return: A connection to the database used to cache results.
        """
        return SQLiteConnection(self.local_experiment_path("_results.db"))

    def create_database(self):
        """
        Save data from output files into the database, overwriting the existing _results.db file if it exists

        These output files must have a particular format. The first line should contain a dictionary, representing
        the parameters of the task. All remaining nonempty lines should be of the form "Key: Value"

        :return: None
        """
        print("Reading all output data into SQL table")

        output_files = self.output_filenames()
        if len(output_files) == 0:
            raise FileNotFoundError("No output files found in " + self._local_directory)

        # Gather the results from all output files
        data = []
        for filename in output_files:
            with open(filename) as input_file:
                lines = input_file.readlines()

                if len(lines) == 0:
                    continue

                try:
                    file_id = int(
                        filename[filename.rfind("/") + 1 :].replace(".out", "")
                    )
                    datum = ast.literal_eval(lines[0])
                    datum.pop("", None)  # remove the list of positional arguments
                    datum["file"] = file_id

                    for line in lines[1:]:
                        if len(line) == 1:
                            continue
                        if ":" not in line:
                            continue

                        key_value_pair = line.split(":", 1)
                        if key_value_pair[1] is "inf" or key_value_pair[1] is "nan":
                            key_value_pair[1] = "-1"

                        try:
                            val = ast.literal_eval(key_value_pair[1].strip())
                            if isinstance(val, int) and val >= 2 ** 63:
                                val = float(val)
                            datum[key_value_pair[0].strip()] = val
                        except (ValueError, SyntaxError):  # Treat as string value
                            datum[key_value_pair[0].strip()] = key_value_pair[1].strip()

                    data.append(datum)
                except SyntaxError:
                    raise SyntaxError("EOL while scanning " + filename)
                except ValueError:
                    raise ValueError("Malformed value while scanning " + filename)

        # Save the results to the database
        with self.get_database() as db:
            # We want to set PRIMARY KEY to be the file column, but pd.DataFrame.to_sql does not support primary keys.
            # https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key
            pandas_sql = pd.io.sql.pandasSQL_builder(db)
            table = pd.io.sql.SQLiteTable(
                "data",
                pandas_sql,
                frame=pd.DataFrame(data),
                index=False,
                if_exists="replace",
                keys="file",
            )
            table.create()
            table.insert()


class SQLiteConnection:
    """
    A connection to an SQLite database that supports using the *with* keyword.
    (unlike using sqlite3.connect directly, which does not support the *with* keyword).

    From https://stackoverflow.com/questions/19522505/using-sqlite3-in-python-with-with-keyword
    """

    def __init__(self, file):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def __exit__(self, exit_type, value, traceback):
        self.conn.commit()
        self.conn.close()


class Arg:
    """
    A class representing an argument to a command.
    """

    def __init__(self, key, value, prefix="--", connector="=", quote_value=True):
        """
        By default, the argument will be included in the command as:
            --key="value"
        
        :param key: Name of the argument
        :param value: Value of the argument
        :param prefix: Prepended to argument name (default "--").
        :param connector: Connector between key and value (default "=")
        :param quote_value: True if the value should be contained within quotes (default True)
        """
        self._key = key
        self._value = value
        self._prefix = prefix
        self._connector = connector
        self._quote_value = quote_value

    def get_args(self):
        """
        :return: The args in a form suitable for including in a command (i.e. ['--key="value"']).
        """
        value = str(self._value)
        if self._quote_value:
            value = '"' + value + '"'

        if self._connector is None:
            return []
        elif self._key is None:
            return [self._prefix + value]
        else:
            return [self._prefix + str(self._key) + self._connector + value]

    def get_table(self):
        """
        :return: The args in a form suitable for a log (i.e. [{key: value}]]).
        """
        if self._key is None:
            return {}
        else:
            return {self._key: self._value}

    @staticmethod
    def private(key, value):
        """
        :return: An argument that will not be included in the command, only the log.
        """
        return Arg(key, value, connector=None)

    @staticmethod
    def positional(value, prefix="", quote_value=True):
        """
        :return: An argument that is positional (i.e. no key).
        """
        return Arg(None, value, prefix=prefix, quote_value=quote_value)

    @staticmethod
    def redirection(key, value):
        """
        :return: An argument suitable for stream redirection.
        """
        return Arg(key, value, prefix="", connector=" ", quote_value=True)

    @staticmethod
    def parse_all_from(key, value):
        """
        Iterate over all arguments generated from the legacy key/value pair.

        Keys that contain | only appear in the log.
        The key '' indicates a list of positional arguments.
        Keys that contain > or < indicate stream redirections.
        """
        if "|" in key:
            yield Arg.private(key, value)
        elif ">" in key or "<" in key:
            yield Arg.redirection(key, value)
        elif key is "":
            yield Arg.private("", value)
            for val in value:
                yield Arg.positional(val)
        else:
            yield Arg(key, value)


class Command:
    """
    A class representing a command with arguments.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a command from a list of arguments.

        All elements of kwargs are processed according to Arg.parse_all_from
        """
        self._args = []
        for arg in args:
            if isinstance(arg, str):
                self._args.append(Arg.positional(arg, quote_value=False))
            else:
                self._args.append(arg)

        for key in kwargs:
            self._args.extend(Arg.parse_all_from(key, kwargs[key]))

    def get_args(self):
        """
        :return: The command in a form suitable for running (i.e. ['python cnfxor.py --key="value"']).
        """
        result = []
        for arg in self._args:
            result += arg.get_args()
        return result

    def get_table(self):
        """
        :return: The command in a form suitable for logging (i.e. {key: value}).
        """
        result = {}
        for arg in self._args:
            result.update(arg.get_table())
        return result

    def __str__(self):
        return " ".join(self.get_args())

    def __or__(self, other):
        """
        Syntactic sugar for pipe
        """
        return Command(self, "|", other)
