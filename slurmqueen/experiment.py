import ast
import io
import os
import pandas as pd
import pandas.io.sql
import sqlite3


class Experiment:
    def __init__(self, command, args, output_argument=">>", log_argument="2>"):
        """
        Initialize an experiment, defined by running the command with parameters given by each dictionary in args.

        The elements of each args dictionary will be passed to the command in each task as --key="value" pairs.
        Keys that contain | will not be passed.
        The key '', if it exists, should point to a list of positional arguments.

        :param command: the command to run in each task, relative to experiment folder (e.g. 'python cnfxor.py')
        :param args: a list of dictionaries; each dictionary defines a new task.
        :param output_argument: the argument used to pass the name of the .out file (defaults to stdout)
        :param log_argument: the argument used to pass the name of the .log file (defaults to stderr)
        """
        self.command = command
        self.args = args
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
        for count, args in enumerate(self._exp.args):
            args = dict(args)

            number = str(count).zfill(
                len(str(len(self._exp.args)))
            )  # Prepend with padded zeros

            input_file = io.open(
                self.local_experiment_path(number + ".in"), "w", newline="\n"
            )

            # Write the arguments as the first line of the output file
            input_file.write(
                'echo "%s" > $(dirname $0)/%s.out'
                % (repr(args).replace('"', '\\"'), number)
            )
            input_file.write("\n")

            input_file.write(self._exp.command)

            args[self._exp.output_argument] = "$(dirname $0)/%s.out" % number
            args[self._exp.log_argument] = "$(dirname $0)/%s.log" % number

            if "" in args:
                for positional_arg in args[""]:
                    input_file.write(' "' + str(positional_arg) + '"')
            for arg_key in args:
                if arg_key == "":  # named argument
                    continue
                if "<" in arg_key or ">" in arg_key:  # stream redirection
                    continue
                if "|" in arg_key:  # private argument
                    continue

                input_file.write(' --%s="%s"' % (str(arg_key), str(args[arg_key])))

            for arg_key in args:
                if "<" in arg_key or ">" in arg_key:  # stream redirection
                    input_file.write(' %s "%s"' % (str(arg_key), str(args[arg_key])))
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
        return len(self._exp.args)

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
