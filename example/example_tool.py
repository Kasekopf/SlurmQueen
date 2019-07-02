import click
import sys


@click.command()
@click.option('--a', type=int, help='The first number to add.')
@click.option('--b', type=int, required=False, default=0, help='The second number to add')
@click.argument('text', type=str)
def do_stuff(a, b, text):
    """
    A simple tool to exercise some of the features of the SlurmQueen framework.
    """

    log('Information that is printed directly appears in the .out file')
    log('Information written stderr appears in the .log file')
    log('TEST LOGGING: positional argument was ' + str(text))

    write_pair_to_out('Sum', a+b)
    write_pair_to_out('Repeated Text', text + text + text)


def write_pair_to_out(key, value):
    print(str(key) + ": " + str(value))


def log(log_string):
    print(log_string, file=sys.stderr)


if __name__ == '__main__':
    do_stuff()
