import sys
import json
import io
import traceback


def run_experiment(param, output_file):
    """
    Do something.

    :param param: The set of parameters provided for this task.
    :param output_file: A file to output the results of this task
    :return: None.
    """
    output_file.write(json.dumps(param) + '\n')


def start_experiment(args):
    """
    Kick off each task specified by the input file.

    :param args: The arguments provided to this node.
    :return: None
    """
    if len(args) < 3:
        print('Unable to start experiment; too few arguments')
        return

    experiment_args = json.loads(args[1])
    experiment_args = dict([(str(k), str(v)) for k, v in experiment_args.items()])

    with io.open(args[2]) as input_file:
        inputs = input_file.readlines()
    changing_args = map(json.loads, inputs)

    for fresh_args in changing_args:
        task_args = dict(experiment_args)
        for key, val in fresh_args.items():
            task_args[str(key)] = str(val)

        with open(task_args['output_file'], 'w') as output_file:
            try:
                run_experiment(task_args, output_file)
            except:
                print('ERROR: ' + str(task_args))
                traceback.print_exc()


if __name__ == "__main__":  # as run on the cluster
    start_experiment(sys.argv)
else:  # as run locally
    from experiment import Experiment, copy_dependencies
    from dashboard import *

    class TestExperiment(Experiment):
        def __init__(self, experiment_id, time_limit, args, changing_args):
            Experiment.__init__(self, 'slurmqueen', experiment_id, __file__.split('\\')[-1], time_limit, args,
                                changing_args=changing_args,
                                modules=[])


    """
    %load_ext autoreload
    %autoreload 2
    %matplotlib inline
    
    test_experiment = TestExperiment('test1', '10:00', args={
        'benchmark': '$HOME/benchmarks/benchmark1.txt',
        'counter': '$HOME/cachet/cachet',
        'failure_probability': 0.125
    }, changing_args=[{'failure_probability': 0.125 * i} for i in range(1, 8)])
    test_experiment.ipython_gui(davinci, 2)
    """
